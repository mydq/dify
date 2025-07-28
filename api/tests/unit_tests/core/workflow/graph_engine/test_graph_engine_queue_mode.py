import queue
import threading
import time
from dataclasses import dataclass
from typing import Any, Optional

from core.app.entities.app_invoke_entities import InvokeFrom
from core.workflow.entities.node_entities import NodeRunResult
from core.workflow.entities.variable_pool import VariablePool
from core.workflow.entities.workflow_node_execution import WorkflowNodeExecutionStatus
from core.workflow.graph_engine.entities.event import (
    GraphRunStartedEvent,
    GraphRunSucceededEvent,
)
from core.workflow.graph_engine.entities.graph import Graph
from core.workflow.graph_engine.entities.graph_runtime_state import GraphRuntimeState
from core.workflow.system_variable import SystemVariable
from models.enums import UserFrom
from models.workflow import WorkflowType


@dataclass
class Task:
    """Represents a node execution task in the queue"""
    node_id: str
    variable_pool: VariablePool
    parallel_info: Optional[dict[str, Any]] = None
    retry_count: int = 0
    dependencies_met: bool = True


class MockTaskQueue:
    """
    Mock implementation of a task queue that tracks all operations
    for testing purposes.
    """
    def __init__(self):
        self._queue = queue.Queue()
        self.put_history = []  # Track all put operations
        self.get_history = []  # Track all get operations
        self.put_count = 0
        self.get_count = 0
        self.task_timestamps = {}  # Track when each task was queued/processed
        
    def put(self, task: Task, block: bool = True, timeout: Optional[float] = None):
        """Add a task to the queue and record the operation"""
        self.put_count += 1
        self.put_history.append({
            'task': task,
            'timestamp': time.time(),
            'thread': threading.current_thread().name
        })
        self.task_timestamps[f"{task.node_id}_queued"] = time.time()
        self._queue.put(task, block=block, timeout=timeout)
        
    def get(self, block: bool = True, timeout: Optional[float] = None) -> Task:
        """Get a task from the queue and record the operation"""
        task = self._queue.get(block=block, timeout=timeout)
        self.get_count += 1
        self.get_history.append({
            'task': task,
            'timestamp': time.time(),
            'thread': threading.current_thread().name
        })
        self.task_timestamps[f"{task.node_id}_processed"] = time.time()
        return task
        
    def empty(self) -> bool:
        """Check if queue is empty"""
        return self._queue.empty()
        
    def qsize(self) -> int:
        """Get queue size"""
        return self._queue.qsize()
        
    def task_done(self):
        """Mark task as done"""
        self._queue.task_done()
        
    def join(self):
        """Wait for all tasks to complete"""
        self._queue.join()


class TestQueueBasedGraphEngine:
    def test_queue_based_execution_with_dependency_injection(self, flask_app_with_db_ctx):
        """
        Test that verifies the queue-based graph engine with dependency injection.
        
        This test creates a diamond-shaped graph:
                 start
                /     \\
            node1     node2
                \\     /
                 node3
                   |
                  end
        
        It verifies:
        1. Custom queue implementation can be injected
        2. Tasks are properly queued and dequeued
        3. Workers process tasks from the injected queue
        4. Dependencies are correctly handled (node3 waits for node1 and node2)
        5. Execution history is properly tracked through the custom queue
        """
        # Arrange
        graph_config = {
            "nodes": [
                {
                    "id": "start",
                    "type": "start",
                    "data": {"variables": []}
                },
                {
                    "id": "node1",
                    "type": "code",
                    "data": {
                        "title": "Node 1",
                        "code": "result = 10",
                        "outputs": {"result": {"type": "number"}}
                    }
                },
                {
                    "id": "node2",
                    "type": "code",
                    "data": {
                        "title": "Node 2",
                        "code": "result = 20",
                        "outputs": {"result": {"type": "number"}}
                    }
                },
                {
                    "id": "node3",
                    "type": "code",
                    "data": {
                        "title": "Node 3",
                        "code": "sum_result = node1_result + node2_result",
                        "variables": [
                            {"variable": "node1_result", "value": "{{#node1.result#}}"},
                            {"variable": "node2_result", "value": "{{#node2.result#}}"}
                        ],
                        "outputs": {"sum_result": {"type": "number"}}
                    }
                },
                {
                    "id": "end",
                    "type": "end",
                    "data": {
                        "outputs": [{"value": "{{#node3.sum_result#}}", "variable": "output"}]
                    }
                }
            ],
            "edges": [
                {"id": "e1", "source": "start", "target": "node1"},
                {"id": "e2", "source": "start", "target": "node2"},
                {"id": "e3", "source": "node1", "target": "node3"},
                {"id": "e4", "source": "node2", "target": "node3"},
                {"id": "e5", "source": "node3", "target": "end"}
            ]
        }
        
        # Create custom task queue
        task_queue = MockTaskQueue()
        event_queue = MockTaskQueue()
        
        # Create graph and runtime state
        graph = Graph.create(graph_config)
        variable_pool = VariablePool(system_variables=SystemVariable())
        
        runtime_state = GraphRuntimeState(
            graph=graph,
            start_at="start",
            variable_pool=variable_pool,
            invoke_from=InvokeFrom.SERVICE_API,
            call_depth=0,
            max_execution_time=600,
            max_execution_steps=1000,
            user_id="test_user",
            workflow_id="test_workflow",
            workflow_type=WorkflowType.WORKFLOW,
            user_from=UserFrom.SERVICE_API
        )
        
        # Track which workers processed which nodes
        worker_assignments = {}
        
        # Mock node executor
        def mock_node_executor(task: Task) -> NodeRunResult:
            """Mock node execution that simulates work and tracks worker assignment"""
            node_id = task.node_id
            worker_name = threading.current_thread().name
            
            if worker_name not in worker_assignments:
                worker_assignments[worker_name] = []
            worker_assignments[worker_name].append(node_id)
            
            # Simulate some work
            time.sleep(0.01)
            
            if node_id == "start":
                return NodeRunResult(
                    status=WorkflowNodeExecutionStatus.SUCCEEDED,
                    edge_source_handle="source"
                )
            elif node_id == "node1":
                return NodeRunResult(
                    status=WorkflowNodeExecutionStatus.SUCCEEDED,
                    outputs={"result": 10},
                    edge_source_handle="source"
                )
            elif node_id == "node2":
                return NodeRunResult(
                    status=WorkflowNodeExecutionStatus.SUCCEEDED,
                    outputs={"result": 20},
                    edge_source_handle="source"
                )
            elif node_id == "node3":
                return NodeRunResult(
                    status=WorkflowNodeExecutionStatus.SUCCEEDED,
                    outputs={"sum_result": 30},
                    edge_source_handle="source"
                )
            elif node_id == "end":
                return NodeRunResult(
                    status=WorkflowNodeExecutionStatus.SUCCEEDED,
                    outputs={"output": 30}
                )
        
        # Act
        from core.workflow.graph_engine.queue_based_graph_engine import QueueBasedGraphEngine
        
        # Create engine with injected queue
        engine = QueueBasedGraphEngine(
            graph=graph,
            runtime_state=runtime_state,
            max_execution_steps=1000,
            max_execution_time=600,
            worker_count=3,
            task_queue=task_queue,  # Inject custom queue
            event_queue=event_queue,  # Inject custom event queue
            node_executor=mock_node_executor  # Inject node executor
        )
        
        events = list(engine.run())
        
        # Assert
        # 1. Verify custom queue was used
        assert engine.task_queue is task_queue
        assert engine.event_queue is event_queue
        
        # 2. Verify all nodes were queued
        queued_nodes = [entry['task'].node_id for entry in task_queue.put_history]
        assert set(queued_nodes) == {"start", "node1", "node2", "node3", "end"}
        
        # 3. Verify all tasks were processed
        processed_nodes = [entry['task'].node_id for entry in task_queue.get_history]
        assert set(processed_nodes) == {"start", "node1", "node2", "node3", "end"}
        assert task_queue.put_count == task_queue.get_count == 5
        
        # 4. Verify dependency handling
        # node3 should be queued after both node1 and node2 are processed
        node1_processed_time = task_queue.task_timestamps.get("node1_processed", 0)
        node2_processed_time = task_queue.task_timestamps.get("node2_processed", 0)
        node3_queued_time = task_queue.task_timestamps.get("node3_queued", 0)
        
        assert node3_queued_time > node1_processed_time
        assert node3_queued_time > node2_processed_time
        
        # 5. Verify multiple workers were used
        assert len(worker_assignments) >= 2  # At least 2 workers should have processed tasks
        
        # 6. Verify parallel execution
        # Check that node1 and node2 were potentially processed by different workers
        node1_thread = next((entry['thread'] for entry in task_queue.get_history 
                           if entry['task'].node_id == "node1"), None)
        node2_thread = next((entry['thread'] for entry in task_queue.get_history 
                           if entry['task'].node_id == "node2"), None)
        
        # Workers exist for both nodes
        assert node1_thread is not None
        assert node2_thread is not None
        
        # 7. Verify correct events
        assert isinstance(events[0], GraphRunStartedEvent)
        assert isinstance(events[-1], GraphRunSucceededEvent)
        assert events[-1].outputs == {"output": 30}
        
        # 8. Verify queue is empty after completion
        assert task_queue.empty()
        assert event_queue.empty()