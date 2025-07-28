"""
Entities for the queue-based graph engine.

This module contains the core data structures and protocols used by the queue-based graph engine.
"""

from dataclasses import dataclass
from typing import Any, Optional, Protocol, TypeVar

from core.workflow.entities.variable_pool import VariablePool

T = TypeVar('T')


@dataclass
class Task:
    """Represents a node execution task in the queue"""
    node_id: str
    variable_pool: VariablePool
    parallel_info: Optional[dict[str, Any]] = None
    retry_count: int = 0
    dependencies_met: bool = True


class TaskQueueProtocol(Protocol[T]):
    """Protocol for task queue implementations"""
    
    def put(self, item: T, block: bool = True, timeout: Optional[float] = None) -> None:
        """Put an item into the queue"""
        ...
        
    def get(self, block: bool = True, timeout: Optional[float] = None) -> T:
        """Get an item from the queue"""
        ...
        
    def empty(self) -> bool:
        """Return True if the queue is empty"""
        ...
        
    def qsize(self) -> int:
        """Return the approximate size of the queue"""
        ...
        
    def task_done(self) -> None:
        """Indicate that a formerly enqueued task is complete"""
        ...
        
    def join(self) -> None:
        """Block until all items in the queue have been processed"""
        ...