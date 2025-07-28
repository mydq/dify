from abc import ABC

from pydantic import BaseModel


class NodeEvent(BaseModel, ABC):
    """Base class for all node events"""

    pass
