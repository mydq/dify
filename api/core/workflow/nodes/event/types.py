from abc import ABC

from core.workflow.graph_engine.entities.event import GraphEngineEvent


class NodeEvent(GraphEngineEvent, ABC):
    """Base class for all node events"""

    pass
