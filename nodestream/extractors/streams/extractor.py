import json
from abc import ABC, abstractmethod
from typing import Any, Iterable

from ...pipeline import Extractor, Flush
from ...subclass_registry import SubclassRegistry
from ...model import JsonLikeDocument


STREAM_CONNECTOR_SUBCLASS_REGISTRY = SubclassRegistry()
STREAM_OBJECT_FORMAT_SUBCLASS_REGISTRY = SubclassRegistry()


@STREAM_CONNECTOR_SUBCLASS_REGISTRY.connect_baseclass
class StreamConnector(ABC):
    @abstractmethod
    async def connect(self):
        raise NotImplementedError

    @abstractmethod
    async def disconnect(self):
        raise NotImplementedError

    @abstractmethod
    async def poll(self, timeout: int, max_records: int) -> Iterable[Any]:
        raise NotImplementedError


@STREAM_OBJECT_FORMAT_SUBCLASS_REGISTRY.connect_baseclass
class StreamRecordFormat(ABC):
    @abstractmethod
    def parse(self, record: Any) -> JsonLikeDocument:
        raise NotImplementedError


class JsonStreamRecordFormat(StreamRecordFormat, name="json"):
    def parse(self, record: Any) -> JsonLikeDocument:
        return json.loads(record)


class StreamExtractor(Extractor):
    """A StreamExtractor implements the standard behavior of polling data from a stream.

    The StreamExtractor requires both a StreamConnector and a StreamRecordFormat to delegate
    to for the actual polling implementation and parsing of the data, respectively.
    """

    @classmethod
    def __declarative_init__(
        cls,
        timeout: int,
        max_records: int,
        record_format: str,
        connector: str,
        **connector_args
    ):
        object_format_cls = STREAM_OBJECT_FORMAT_SUBCLASS_REGISTRY.get(record_format)
        connector_cls = STREAM_CONNECTOR_SUBCLASS_REGISTRY.get(connector)
        return cls(
            timeout=timeout,
            max_records=max_records,
            record_format=object_format_cls(),
            connector=connector_cls(**connector_args),
        )

    def __init__(
        self,
        connector: StreamConnector,
        record_format: StreamRecordFormat,
        timeout: int,
        max_records: int,
    ):
        self.connector = connector
        self.record_format = record_format
        self.timeout = timeout
        self.max_records = max_records

    async def poll(self):
        return await self.connector.poll(
            timeout=self.timeout, max_records=self.max_records
        )

    async def extract_records(self):
        await self.connector.connect()
        try:
            results = list(await self.poll())
            if len(results) == 0:
                yield Flush
            else:
                for record in results:
                    yield self.record_format.parse(record)
        finally:
            await self.connector.disconnect()
