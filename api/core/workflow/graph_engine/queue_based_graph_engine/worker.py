"""
Worker implementation for the queue-based graph engine.

This module contains the worker logic that processes tasks from the queue.
"""

import logging
import queue
import threading
from collections.abc import Callable, Generator
from typing import Optional

from core.workflow.graph_engine.entities.event import GraphEngineEvent
from core.workflow.nodes.event import RunCompletedEvent

from .entities import Task, TaskQueueProtocol

logger = logging.getLogger(__name__)


class GraphEngineWorker:
    """
    Worker that processes tasks from the task queue.
    """

    def __init__(
        self,
        worker_id: int,
        task_queue: TaskQueueProtocol[Task],
        event_queue: TaskQueueProtocol[GraphEngineEvent],
        node_executor: Callable[[Task], Generator[GraphEngineEvent, None, None]],
        stop_event: threading.Event,
        lock: threading.Lock,
        pending_tasks_counter: dict[str, int],
        completed_nodes: set[str],
        node_outputs: dict[str, dict],
        execution_steps_counter: dict[str, int],
    ):
        self.worker_id = worker_id
        self.task_queue = task_queue
        self.event_queue = event_queue
        self.node_executor = node_executor
        self.stop_event = stop_event
        self.lock = lock
        self.pending_tasks_counter = pending_tasks_counter
        self.completed_nodes = completed_nodes
        self.node_outputs = node_outputs
        self.execution_steps_counter = execution_steps_counter

        self.thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the worker thread"""
        self.thread = threading.Thread(target=self._worker_loop, name=f"QueueWorker-{self.worker_id}", daemon=True)
        self.thread.start()

    def join(self, timeout: Optional[float] = None) -> None:
        """Wait for the worker thread to finish"""
        if self.thread:
            self.thread.join(timeout)

    def _worker_loop(self) -> None:
        """Worker thread main loop"""
        while not self.stop_event.is_set():
            try:
                task = self.task_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            try:
                self._process_task(task)
            except Exception as e:
                logger.exception("Error processing task %s", task.node_id)
                # For now, just continue - in a full implementation we'd emit failure events
            finally:
                self.task_queue.task_done()
                with self.lock:
                    self.pending_tasks_counter["count"] -= 1

    def _process_task(self, task: Task) -> None:
        """Process a single task"""
        node_id = task.node_id

        # Increment execution steps
        with self.lock:
            self.execution_steps_counter["count"] += 1

        # Execute node - this returns a generator of GraphEngineEvents
        event_generator = self.node_executor(task)

        # Process all events from the node
        final_result = None
        for event in event_generator:
            # Forward the event to the event queue for consumers
            self.event_queue.put(event)

            # Check if this is a RunCompletedEvent which contains the final result
            if isinstance(event, RunCompletedEvent):
                final_result = event.run_result
                break

        # Store outputs from the final result
        if final_result and final_result.outputs:
            with self.lock:
                self.node_outputs[node_id] = dict(final_result.outputs)
                # Update variable pool with node outputs
                for key, value in final_result.outputs.items():
                    task.variable_pool.add((node_id, key), value)

        # Mark node as completed
        with self.lock:
            self.completed_nodes.add(node_id)

        # Successor nodes are now queued by the main engine when it receives RunCompletedEvent
