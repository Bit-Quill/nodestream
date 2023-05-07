from typing import Any, Dict, Iterable, Type

from yaml import SafeLoader

from ..model import InterpreterContext
from .value_provider import StaticValueOrValueProvider, ValueProvider


class StringFormattingValueProvider(ValueProvider):
    @classmethod
    def install_yaml_tag(cls, loader: Type[SafeLoader]):
        loader.add_constructor(
            "!format", lambda loader, node: cls(**loader.construct_mapping(node))
        )

    def __init__(
        self,
        fmt: StaticValueOrValueProvider,
        **subs: Dict[str, StaticValueOrValueProvider],
    ) -> None:
        self.fmt = ValueProvider.garuntee_value_provider(fmt)
        self.subs = ValueProvider.garuntee_provider_dictionary(subs)

    def single_value(self, context: InterpreterContext) -> Any:
        if (fmt := self.fmt.single_value(context)) is None:
            return None

        subs = {
            field: provider.single_value(context)
            for field, provider in self.subs.items()
        }

        return fmt.format(**subs)

    def many_values(self, context: InterpreterContext) -> Iterable[Any]:
        value = self.single_value(context)
        return [value] if value else []
