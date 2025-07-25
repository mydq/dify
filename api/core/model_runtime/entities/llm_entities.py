from collections.abc import Mapping, Sequence
from decimal import Decimal
from enum import StrEnum
from typing import Any, Optional

from pydantic import BaseModel, Field

from core.model_runtime.entities.message_entities import AssistantPromptMessage, PromptMessage
from core.model_runtime.entities.model_entities import ModelUsage, PriceInfo


class LLMMode(StrEnum):
    """
    Enum class for large language model mode.
    """

    COMPLETION = "completion"
    CHAT = "chat"


class LLMUsage(ModelUsage):
    """
    Model class for llm usage.
    """

    prompt_tokens: int
    prompt_unit_price: Decimal
    prompt_price_unit: Decimal
    prompt_price: Decimal
    completion_tokens: int
    completion_unit_price: Decimal
    completion_price_unit: Decimal
    completion_price: Decimal
    total_tokens: int
    total_price: Decimal
    currency: str
    latency: float

    @classmethod
    def empty_usage(cls):
        return cls(
            prompt_tokens=0,
            prompt_unit_price=Decimal("0.0"),
            prompt_price_unit=Decimal("0.0"),
            prompt_price=Decimal("0.0"),
            completion_tokens=0,
            completion_unit_price=Decimal("0.0"),
            completion_price_unit=Decimal("0.0"),
            completion_price=Decimal("0.0"),
            total_tokens=0,
            total_price=Decimal("0.0"),
            currency="USD",
            latency=0.0,
        )

    @classmethod
    def from_metadata(cls, metadata: dict) -> "LLMUsage":
        """
        Create LLMUsage instance from metadata dictionary with default values.

        Args:
            metadata: Dictionary containing usage metadata

        Returns:
            LLMUsage instance with values from metadata or defaults
        """
        total_tokens = metadata.get("total_tokens", 0)
        completion_tokens = metadata.get("completion_tokens", 0)
        if total_tokens > 0 and completion_tokens == 0:
            completion_tokens = total_tokens

        return cls(
            prompt_tokens=metadata.get("prompt_tokens", 0),
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
            prompt_unit_price=Decimal(str(metadata.get("prompt_unit_price", 0))),
            completion_unit_price=Decimal(str(metadata.get("completion_unit_price", 0))),
            total_price=Decimal(str(metadata.get("total_price", 0))),
            currency=metadata.get("currency", "USD"),
            prompt_price_unit=Decimal(str(metadata.get("prompt_price_unit", 0))),
            completion_price_unit=Decimal(str(metadata.get("completion_price_unit", 0))),
            prompt_price=Decimal(str(metadata.get("prompt_price", 0))),
            completion_price=Decimal(str(metadata.get("completion_price", 0))),
            latency=metadata.get("latency", 0.0),
        )

    def plus(self, other: "LLMUsage") -> "LLMUsage":
        """
        Add two LLMUsage instances together.

        :param other: Another LLMUsage instance to add
        :return: A new LLMUsage instance with summed values
        """
        if self.total_tokens == 0:
            return other
        else:
            return LLMUsage(
                prompt_tokens=self.prompt_tokens + other.prompt_tokens,
                prompt_unit_price=other.prompt_unit_price,
                prompt_price_unit=other.prompt_price_unit,
                prompt_price=self.prompt_price + other.prompt_price,
                completion_tokens=self.completion_tokens + other.completion_tokens,
                completion_unit_price=other.completion_unit_price,
                completion_price_unit=other.completion_price_unit,
                completion_price=self.completion_price + other.completion_price,
                total_tokens=self.total_tokens + other.total_tokens,
                total_price=self.total_price + other.total_price,
                currency=other.currency,
                latency=self.latency + other.latency,
            )

    def __add__(self, other: "LLMUsage") -> "LLMUsage":
        """
        Overload the + operator to add two LLMUsage instances.

        :param other: Another LLMUsage instance to add
        :return: A new LLMUsage instance with summed values
        """
        return self.plus(other)


class LLMResult(BaseModel):
    """
    Model class for llm result.
    """

    id: Optional[str] = None
    model: str
    prompt_messages: Sequence[PromptMessage] = Field(default_factory=list)
    message: AssistantPromptMessage
    usage: LLMUsage
    system_fingerprint: Optional[str] = None


class LLMStructuredOutput(BaseModel):
    """
    Model class for llm structured output.
    """

    structured_output: Optional[Mapping[str, Any]] = None


class LLMResultWithStructuredOutput(LLMResult, LLMStructuredOutput):
    """
    Model class for llm result with structured output.
    """


class LLMResultChunkDelta(BaseModel):
    """
    Model class for llm result chunk delta.
    """

    index: int
    message: AssistantPromptMessage
    usage: Optional[LLMUsage] = None
    finish_reason: Optional[str] = None


class LLMResultChunk(BaseModel):
    """
    Model class for llm result chunk.
    """

    model: str
    prompt_messages: Sequence[PromptMessage] = Field(default_factory=list)
    system_fingerprint: Optional[str] = None
    delta: LLMResultChunkDelta


class LLMResultChunkWithStructuredOutput(LLMResultChunk, LLMStructuredOutput):
    """
    Model class for llm result chunk with structured output.
    """


class NumTokensResult(PriceInfo):
    """
    Model class for number of tokens result.
    """

    tokens: int
