from dataclasses import dataclass
from typing import Union

from .settings import DEFAULT_SERVICE_URL


@dataclass
class EmbeddingSchema:
    distance: str
    dim: int


@dataclass
class ExtractorSchema:
    outputs: dict[str, Union[EmbeddingSchema, dict]]


class Extractor:
    def __init__(
        self, name: str, description: str, input_params: dict, outputs: ExtractorSchema, input_mime_types: list[str]
    ):
        self.name = name
        self.description = description
        self.input_params = input_params
        self.outputs = outputs
        self.input_mime_types = input_mime_types

    @classmethod
    def from_dict(cls, data):
        return Extractor(
            name=data["name"],
            description=data["description"],
            input_params=data["input_params"],
            input_mime_types=data["input_mime_types"],
            outputs=data["outputs"],
        )

    def __repr__(self) -> str:
        return f"Extractor(name={self.name}, description={self.description}, input_params={self.input_params}, input_mime_types={self.input_mime_types}, outputs={self.outputs})"

    def __str__(self) -> str:
        return self.__repr__()
