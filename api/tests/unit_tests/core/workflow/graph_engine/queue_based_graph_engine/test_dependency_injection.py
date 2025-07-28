import time

from core.workflow.entities.variable_pool import VariablePool
from core.workflow.graph_engine.entities.event import GraphRunStartedEvent, GraphRunSucceededEvent
from core.workflow.graph_engine.entities.graph import Graph
from core.workflow.graph_engine.entities.graph_runtime_state import GraphRuntimeState
from core.workflow.graph_engine.queue_based_graph_engine import QueueBasedGraphEngine
from core.workflow.system_variable import SystemVariable

from .conftest import MockEventQueue, MockTaskQueue, create_mock_node_executor


class TestQueueBasedGraphEngineDependencyInjection:
    def test_queue_based_execution_with_dependency_injection(self, app):
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
                {"id": "start", "data": {"type": "start", "title": "Start", "variables": []}},
                {
                    "id": "node1",
                    "data": {
                        "type": "code",
                        "title": "Node 1",
                        "code": "result = 10",
                        "outputs": {"result": {"type": "number"}},
                    },
                },
                {
                    "id": "node2",
                    "data": {
                        "type": "code",
                        "title": "Node 2",
                        "code": "result = 20",
                        "outputs": {"result": {"type": "number"}},
                    },
                },
                {
                    "id": "node3",
                    "data": {
                        "type": "code",
                        "title": "Node 3",
                        "code": "sum_result = node1_result + node2_result",
                        "variables": [
                            {"variable": "node1_result", "value": "{{#node1.result#}}"},
                            {"variable": "node2_result", "value": "{{#node2.result#}}"},
                        ],
                        "outputs": {"sum_result": {"type": "number"}},
                    },
                },
                {
                    "id": "end",
                    "data": {
                        "type": "end",
                        "title": "End",
                        "outputs": [{"value_selector": ["node3", "sum_result"], "variable": "output"}],
                    },
                },
            ],
            "edges": [
                {"id": "e1", "source": "start", "target": "node1"},
                {"id": "e2", "source": "start", "target": "node2"},
                {"id": "e3", "source": "node1", "target": "node3"},
                {"id": "e4", "source": "node2", "target": "node3"},
                {"id": "e5", "source": "node3", "target": "end"},
            ],
        }

        # Create custom task queue
        task_queue = MockTaskQueue()
        event_queue = MockEventQueue()

        # Create graph and runtime state
        graph = Graph.init(graph_config=graph_config)
        variable_pool = VariablePool(
            system_variables=SystemVariable(
                user_id="test_user", app_id="test_app", workflow_id="test_workflow", files=[]
            ),
            user_inputs={"query": "test"},
        )

        runtime_state = GraphRuntimeState(variable_pool=variable_pool, start_at=time.perf_counter())

        # Create mock node executor and get worker assignments tracker
        mock_node_executor, worker_assignments = create_mock_node_executor()

        # Act
        # Create engine with injected queue
        engine = QueueBasedGraphEngine(
            graph=graph,
            runtime_state=runtime_state,
            max_execution_steps=1000,
            max_execution_time=600,
            worker_count=3,
            task_queue=task_queue,  # Inject custom queue
            event_queue=event_queue,  # Inject custom event queue
            node_executor=mock_node_executor,  # Inject node executor
        )

        events = list(engine.run())

        # Assert
        # 1. Verify custom queue was used
        assert engine.task_queue is task_queue
        assert engine.event_queue is event_queue

        # 2. Verify all nodes were queued
        queued_nodes = [entry["task"].node_id for entry in task_queue.put_history]
        assert set(queued_nodes) == {"start", "node1", "node2", "node3", "end"}

        # 3. Verify all tasks were processed
        processed_nodes = [entry["task"].node_id for entry in task_queue.get_history]
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
        node1_thread = next(
            (entry["thread"] for entry in task_queue.get_history if entry["task"].node_id == "node1"), None
        )
        node2_thread = next(
            (entry["thread"] for entry in task_queue.get_history if entry["task"].node_id == "node2"), None
        )

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
