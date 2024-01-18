from logging import getLogger
from typing import Iterable
import boto3

from ...model import IngestionHook, Node, RelationshipWithNodes, TimeToLiveConfiguration
from ...schema.indexes import FieldIndex, KeyIndex
from ..query_executor import (
    OperationOnNodeIdentity,
    OperationOnRelationshipIdentity,
    QueryExecutor,
)
from .ingest_query_builder import NeptuneDBIngestQueryBuilder
from .query import Query


class NeptuneQueryExecutor(QueryExecutor):
    def __init__(
        self,
        client: boto3.client,
        ingest_query_builder: NeptuneDBIngestQueryBuilder
    ) -> None:
        self.client = client
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
        # print(f"NeptuneQueryExecutor.upsert_nodes_in_bulk_with_same_operation - batched query:\n {batched_query}")
        await self.execute(
            batched_query.as_query(self.ingest_query_builder.apoc_iterate),
            log_result=True,
        )

    async def upsert_relationships_in_bulk_of_same_operation(
        self,
        shape: OperationOnRelationshipIdentity,
        relationships: Iterable[RelationshipWithNodes],
    ):
        batched_query = (
            self.ingest_query_builder.generate_batch_update_relationship_query_batch(
                shape, relationships
            )
        )

        # print(f"NeptuneQueryExecutor.upsert_relationships_in_bulk_of_same_operation - batched query:\n {batched_query}")
        await self.execute(
            batched_query.as_query(self.ingest_query_builder.apoc_iterate),
            log_result=True,
        )

    async def upsert_key_index(self, index: KeyIndex):
        pass

    async def upsert_field_index(self, index: FieldIndex):
        pass

    async def perform_ttl_op(self, config: TimeToLiveConfiguration):
        query = self.ingest_query_builder.generate_ttl_query_from_configuration(config)
        await self.execute(query)

    async def execute_hook(self, hook: IngestionHook):
        query_string, params = hook.as_cypher_query_and_parameters()
        await self.execute(Query(query_string, params))

    async def execute(self, query: Query, log_result: bool = False):
        self.logger.debug(
            "Executing Cypher Query to Neptune",
            extra={
                "query": query.query_statement,
                # "uri": self.driver._pool.address.host,
            },
        )

        result = await self.execute_query(
            query.query_statement,
            query.parameters,
            # database_=self.database_name,
        )
        if log_result:
            pass
            # print("Will log result here")
            # for record in result.records:
            #     self.logger.info(
            #         "Gathered Query Results",
            #         extra=dict(**record, query=query.query_statement),
            #     )

    # not using the async library yet, just testing running neptune queries here
    async def execute_query(self, query_statement, query_parameters):
        print(f"\nNeptune executing query.\n")
        # print(f"Query statement: \n{query_statement}")
        # print(f"Query parameters:\n{query_parameters}")

        # Find Techonology likes persion relationship
        response = self.client.execute_open_cypher_query(
            openCypherQuery='MATCH (p:Person)-[:LIKES]-(t:Technology) RETURN p;',
        )
        print(f"Response ** \n{response}")


        response = self.client.execute_open_cypher_query(
            openCypherQuery="MERGE (node: Person {first_name : 'jack', last_name : 'summer'}) SET node += {last_ingested_at: '2024-01-16 00:24:49.078545+00:00', last_ingested_by_test_at: '2024-01-16 00:24:49.077536+00:00', was_ingested_by_test: true, age: 38}",
        )
        print(f"\nResponse ** \n{response}")


        return []
