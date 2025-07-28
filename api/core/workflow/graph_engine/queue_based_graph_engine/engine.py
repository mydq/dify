"""
Main queue-based graph engine implementation.

This module contains the core QueueBasedGraphEngine class that orchestrates
the queue-based execution of workflow graphs.
"""

import logging
import queue
import threading
import time
from collections import defaultdict
from collections.abc import Callable, Generator
from typing import Any, Optional

from core.app.entities.app_invoke_entities import InvokeFrom
from core.workflow.entities.node_entities import NodeRunResult
from core.workflow.graph_engine.entities.event import (
    GraphEngineEvent,
    GraphRunFailedEvent,
    GraphRunStartedEvent,
    GraphRunSucceededEvent,
)
from core.workflow.graph_engine.entities.graph import Graph
from core.workflow.graph_engine.entities.graph_init_params import GraphInitParams
from core.workflow.graph_engine.entities.graph_runtime_state import GraphRuntimeState
from core.workflow.nodes import NodeType
from core.workflow.nodes.event import NodeEvent, RunCompletedEvent
from core.workflow.nodes.node_mapping import NODE_TYPE_CLASSES_MAPPING
from models.enums import UserFrom
from models.workflow import WorkflowType

from .entities import Task, TaskQueueProtocol
from .worker import GraphEngineWorker

logger = logging.getLogger(__name__)


class QueueBasedGraphEngine:
    """
    A queue-based graph execution engine that uses worker threads to process nodes.
    This avoids deep recursion and enables better scalability.
    """

    def __init__(
        self,
        graph: Graph,
        runtime_state: GraphRuntimeState,
        max_execution_steps: int,
        max_execution_time: int,
        worker_count: int = 2,
        task_queue: Optional[TaskQueueProtocol[Task]] = None,
        event_queue: Optional[TaskQueueProtocol[GraphEngineEvent]] = None,
        node_executor: Optional[Callable[[Task], Generator[NodeEvent, None, None]]] = None,
    ):
        self.graph = graph
        self.runtime_state = runtime_state
        self.max_execution_steps = max_execution_steps
        self.max_execution_time = max_execution_time
        self.worker_count = worker_count

        # Use injected queues or create default ones
        self.task_queue: TaskQueueProtocol[Task] = task_queue if task_queue is not None else queue.Queue()
        self.event_queue: TaskQueueProtocol[GraphEngineEvent] = (
            event_queue if event_queue is not None else queue.Queue()
        )

        # Use injected node executor or default implementation
        self.node_executor = node_executor if node_executor is not None else self._default_node_executor

        # Track dependencies
        self.in_degree = defaultdict(int)

        # Shared state for workers (using dicts for mutable reference sharing)
        self.pending_tasks_counter = {"count": 0}
        self.execution_steps_counter = {"count": 0}
        self.lock = threading.Lock()

        # Track completed nodes for dependency resolution
        self.completed_nodes = set()

        # Workers
        self.workers = []
        self.stop_workers = threading.Event()

        # Track node outputs
        self.node_outputs = {}

    @property
    def pending_tasks(self) -> int:
        """Get the current number of pending tasks"""
        return self.pending_tasks_counter["count"]

    @property
    def execution_steps(self) -> int:
        """Get the current number of execution steps"""
        return self.execution_steps_counter["count"]

    def run(self):
        """Main entry point that yields events as the graph executes"""
        # Emit graph start event
        self.event_queue.put(GraphRunStartedEvent())

        # Calculate in-degrees
        self._calculate_in_degrees()

        # Start workers
        self._start_workers()

        # Queue initial tasks (nodes with in-degree 0)
        self._queue_initial_tasks()

        # Process events
        start_time = time.time()
        failed = False
        error_message = None

        try:
            while True:
                # Check timeouts
                if time.time() - start_time > self.max_execution_time:
                    error_message = f"Max execution time {self.max_execution_time}s reached."
                    failed = True
                    break

                if self.execution_steps > self.max_execution_steps:
                    error_message = f"Max steps {self.max_execution_steps} reached."
                    failed = True
                    break

                # Get event with timeout
                try:
                    event = self.event_queue.get(timeout=0.1)
                    yield event

                except queue.Empty:
                    # Check if we're done
                    with self.lock:
                        if self.pending_tasks == 0 and self.task_queue.empty():
                            break

        finally:
            # Stop workers
            self.stop_workers.set()
            for worker in self.workers:
                worker.join()

        # Emit final event
        if failed:
            yield GraphRunFailedEvent(error=error_message or "Unknown error", exceptions_count=0)
        else:
            # Get outputs from end node
            outputs = self._get_graph_outputs()
            yield GraphRunSucceededEvent(outputs=outputs)

    def _calculate_in_degrees(self) -> None:
        """Calculate in-degree for each node"""
        for source_node_id, edges in self.graph.edge_mapping.items():
            for edge in edges:
                self.in_degree[edge.target_node_id] += 1

    def _queue_initial_tasks(self) -> None:
        """Queue tasks for nodes with no dependencies"""
        for node_id in self.graph.node_id_config_mapping:
            if self.in_degree[node_id] == 0:
                self._enqueue_task(node_id)

    def _enqueue_task(self, node_id: str) -> None:
        """Add a task to the queue"""
        with self.lock:
            self.pending_tasks_counter["count"] += 1

        task = Task(node_id=node_id, variable_pool=self.runtime_state.variable_pool.model_copy())
        self.task_queue.put(task)

    def _start_workers(self) -> None:
        """Start worker threads"""
        for i in range(self.worker_count):
            worker = GraphEngineWorker(
                worker_id=i,
                task_queue=self.task_queue,
                event_queue=self.event_queue,
                node_executor=self.node_executor,
                stop_event=self.stop_workers,
                lock=self.lock,
                pending_tasks_counter=self.pending_tasks_counter,
                completed_nodes=self.completed_nodes,
                node_outputs=self.node_outputs,
                execution_steps_counter=self.execution_steps_counter,
                successor_callback=self._queue_successors,
            )
            worker.start()
            self.workers.append(worker)

    def _queue_successors(self, node_id: str) -> None:
        """Queue successor nodes whose dependencies are satisfied"""
        # Get outgoing edges from this node
        outgoing_edges = self.graph.edge_mapping.get(node_id, [])

        for edge in outgoing_edges:
            target_id = edge.target_node_id

            # Check if all dependencies are met for the target node
            dependencies_met = True

            # Check all incoming edges to the target node
            for source_id, edges_list in self.graph.edge_mapping.items():
                for check_edge in edges_list:
                    if check_edge.target_node_id == target_id:
                        if source_id not in self.completed_nodes:
                            dependencies_met = False
                            break
                if not dependencies_met:
                    break

            if dependencies_met:
                self._enqueue_task(target_id)

    def _default_node_executor(self, task: Task) -> NodeRunResult:
        """Default node executor implementation that runs real nodes"""
        node_id = task.node_id
        node_config = self.graph.node_id_config_mapping.get(node_id)
        if not node_config:
            raise ValueError(f"Node {node_id} config not found")

        # Get node type and version
        node_type = NodeType(node_config.get("data", {}).get("type"))
        node_version = node_config.get("data", {}).get("version", "1")

        # Get node class
        node_cls = NODE_TYPE_CLASSES_MAPPING[node_type][node_version]

        # Create graph init params for the node
        init_params = GraphInitParams(
            tenant_id="test_tenant",
            app_id="test_app",
            workflow_type=WorkflowType.WORKFLOW,
            workflow_id="test_workflow",
            graph_config={},  # Not needed for simple nodes
            user_id="test_user",
            user_from=UserFrom.ACCOUNT,
            invoke_from=InvokeFrom.WEB_APP,
            call_depth=0,
        )

        # Instantiate the node
        node = node_cls(
            id=node_id,
            config=node_config,
            graph_init_params=init_params,
            graph=self.graph,
            graph_runtime_state=self.runtime_state,
            previous_node_id=None,
            thread_pool_id=None,
        )

        # Initialize node data
        node.init_node_data(node_config.get("data", {}))

        # Run the node
        result = node.run()

        # Handle both NodeRunResult and Generator cases
        if hasattr(result, "__next__"):  # It's a generator returning NodeEvents
            # Return the generator as-is - the worker will handle events
            return result
        else:
            # It's a NodeRunResult - convert to RunCompletedEvent (which is a NodeEvent)
            def result_to_event_generator():
                yield RunCompletedEvent(run_result=result)

            return result_to_event_generator()

    def _get_graph_outputs(self) -> dict[str, Any]:
        """Get outputs from the graph (from end nodes)"""
        outputs = {}

        # Find end nodes and collect their outputs
        for node_id, config in self.graph.node_id_config_mapping.items():
            if config.get("data", {}).get("type") == "end":
                if node_id in self.node_outputs:
                    outputs.update(self.node_outputs[node_id])

        return outputs
