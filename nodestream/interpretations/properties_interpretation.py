from typing import Any, Dict, Optional, Iterable

from ..model import (
    InterpreterContext,
    GraphObjectShape,
    UnknownTypeMarker,
    PropertyMetadataSet,
    GraphObjectType,
)
from ..model.value_provider import StaticValueOrValueProvider, ValueProvider
from .interpretation import Interpretation


class PropertiesInterpretation(Interpretation, name="properties"):
    """Stores additional properties onto the source node."""

    __slots__ = ("properties", "norm_args")

    def __init__(
        self,
        properties: Dict[str, StaticValueOrValueProvider],
        normalization: Optional[Dict[str, Any]] = None,
    ):
        self.properties = ValueProvider.garuntee_provider_dictionary(properties)
        self.norm_args = normalization or {}

    def interpret(self, context: InterpreterContext):
        source = context.desired_ingest.source
        source.properties.apply_providers(context, self.properties, **self.norm_args)

    def gather_object_shapes(self) -> Iterable[GraphObjectShape]:
        yield GraphObjectShape(
            graph_object_type=GraphObjectType.NODE,
            object_type=UnknownTypeMarker.source_node(),
            properties=PropertyMetadataSet.from_names(self.properties.keys()),
        )