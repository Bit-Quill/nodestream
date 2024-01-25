from logging import getLogger
from typing import Iterable
from ...model import IngestionHook, Node, RelationshipWithNodes, TimeToLiveConfiguration
from ...schema.indexes import FieldIndex, KeyIndex
from ..query_executor import (
    OperationOnNodeIdentity,
    OperationOnRelationshipIdentity,
    QueryExecutor,
)
from .ingest_query_builder import NeptuneDBIngestQueryBuilder
from .query import Query, QueryBatch
from aiobotocore.session import get_session
import json



class NeptuneQueryExecutor(QueryExecutor):
    def __init__(
        self,
        region,
        host,
        ingest_query_builder: NeptuneDBIngestQueryBuilder
    ) -> None:
        self.session = get_session()
        self.region = region
        self.host = host
        self.ingest_query_builder = ingest_query_builder
        self.logger = getLogger(self.__class__.__name__)

    async def upsert_nodes_in_bulk_with_same_operation(
        self, operation: OperationOnNodeIdentity, nodes: Iterable[Node]
    ):
        batched_query = (
            self.ingest_query_builder.generate_batch_update_node_operation_batch(
                operation, nodes
            )
        )
        await self.execute(batched_query.as_query())

    async def upsert_relationships_in_bulk_of_same_operation(
        self,
        shape: OperationOnRelationshipIdentity,
        relationships: Iterable[RelationshipWithNodes],
    ):
        queries = (
            self.ingest_query_builder.generate_batch_update_relationship_query_batch(
                shape, relationships
            )
        )
        await self.execute(queries.as_query())

    async def upsert_key_index(self, index: KeyIndex):
        self.logger.warning(f"upsert_key_index not implemented: Neptune does not need to update index keys.")

    async def upsert_field_index(self, index: FieldIndex):
        self.logger.warning("upsert_field_index not implemented: Neptune does not need to update index keys.")

    async def perform_ttl_op(self, config: TimeToLiveConfiguration):
        self.logger.warning("perform_ttl_op not implemented, no query was executed.")

    async def execute_hook(self, hook: IngestionHook):
        self.logger.warning("execute_hook not implemented, no query was executed.")

    # not using the async library yet, just testing running neptune queries here
    async def execute(self, query: QueryBatch, log_result: bool = False):

        async with self.session.create_client("neptunedata", region_name=self.region, endpoint_url=self.host) as client:
            try:
                response = await client.execute_open_cypher_query(
                    openCypherQuery=query.query_statement,
                    # Use json.dumps() to warp dict's key/values in double quotes.
                    parameters=json.dumps(query.parameters)
                )
                self.logger.info(response)
            except Exception as e:
                self.logger.error(f'Failed at query: {query}')
                raise e
