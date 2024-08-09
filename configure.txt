import neo4j
from langchain_community.embeddings import SpacyEmbeddings
from langchain_community.vectorstores import Neo4jVector

from benchmark.dataset import Dataset
from engine.base_client.configure import BaseConfigurator
from engine.clients.neo4j.config import NEO4j_URL, NEO4j_USER, NEO4j_PASSWORD, NEO4j_DATABASE, NEO4j_INDEX


class Neo4jConfigurator(BaseConfigurator):

    def __init__(self, host, collection_params: dict, connection_params: dict):
        super().__init__(host, collection_params, connection_params)
        self.client = Neo4jVector(
            url=NEO4j_URL,
            username=NEO4j_USER,
            password=NEO4j_PASSWORD,
            embedding=SpacyEmbeddings(),
            database=NEO4j_DATABASE,
            index_name=NEO4j_INDEX,
            node_label=NEO4j_INDEX,
        )
        self.session = neo4j.GraphDatabase. \
            driver(NEO4j_URL, auth=(NEO4j_USER, NEO4j_PASSWORD)).session(database=NEO4j_DATABASE)

    def clean(self):
        pass
        exist = self.client.retrieve_existing_index()
        if exist:
            drop_query = (
                "DROP INDEX $index_name"
            )
            param = {
                "index_name": NEO4j_INDEX
            }
            self.session.run(drop_query, param)
        #
        # index_query = (
        #     "SHOW INDEXES YIELD name, type, labelsOrTypes, properties, options "
        #     "WHERE type = 'VECTOR' AND (name = $index_name "
        #     "OR (labelsOrTypes[0] = $node_label AND "
        #     "properties[0] = $embedding_node_property)) "
        #     "RETURN name, labelsOrTypes, properties, options ",
        # )
        # params = {
        #     "index_name": NEO4j_INDEX,
        #     "node_label": NEO4j_INDEX,
        #     "embedding_node_property": 'embedding',
        # }
        # result = self.client.run(index_query, params)

    def recreate(self, dataset: Dataset, collection_params):
        self.client.embedding_dimension = dataset.config.vector_size
        self.client.create_new_index()
        # index_query = (
        #     "CALL db.index.vector.createNodeIndex("
        #     "$index_name,"
        #     "$node_name,"
        #     "$embedding_node_property,"
        #     "toInteger($embedding_dimension),"
        #     "$similarity_metric)"
        # )
        # param = {
        #     "index_name": NEO4j_INDEX,
        #     "node_name": NEO4j_INDEX + str(time.time()),
        #     "embedding_node_property": "embedding",
        #     "embedding_dimension": dataset.config.vector_size,
        #     "similarity_metric": dataset.config.distance
        # }
        # self.client.run(index_query, param)
