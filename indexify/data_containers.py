from enum import Enum
from typing import List
from dataclasses import dataclass, field


@dataclass
class TextChunk:
    text: str
    metadata: dict[str, any] = field(default_factory=dict)
    score: float = 0.0

    def to_dict(self):
        return {"text": self.text, "metadata": self.metadata}


@dataclass
class SearchResult:
    results: List[TextChunk]
