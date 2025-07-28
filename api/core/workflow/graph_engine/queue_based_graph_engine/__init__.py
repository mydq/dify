"""
Queue-based graph engine module.

This module provides a queue-based alternative to the recursive graph engine,
enabling better scalability and concurrency control.
"""

from .engine import QueueBasedGraphEngine
from .entities import Task, TaskQueueProtocol
from .worker import GraphEngineWorker

__all__ = ["GraphEngineWorker", "QueueBasedGraphEngine", "Task", "TaskQueueProtocol"]
