import multiprocessing as mp
import uuid
from typing import List, Tuple

import neo4j
from langchain_community.embeddings import SpacyEmbeddings
from langchain_community.vectorstores import Neo4jVector

from dataset_reader.base_reader import Query
from engine.base_client.search import BaseSearcher
from engine.clients.neo4j.config import NEO4j_URL, NEO4j_USER, NEO4j_PASSWORD, NEO4j_DATABASE, NEO4j_INDEX


class Neo4jSearcher(BaseSearcher):
    search_params = {}
    client: Neo4jVector = None

    @classmethod
    def get_mp_start_method(cls):
        return "forkserver" if "forkserver" in mp.get_all_start_methods() else "spawn"

    @classmethod
    def init_client(cls, host, distance, connection_params: dict, search_params: dict):
        cls.client = Neo4jVector(
            url=NEO4j_URL,
            username=NEO4j_USER,
            password=NEO4j_PASSWORD,
            embedding=SpacyEmbeddings(),
            database=NEO4j_DATABASE,
            index_name=NEO4j_INDEX,
            node_label=NEO4j_INDEX,
        )
        # cls.client = neo4j.GraphDatabase. \
        #     driver(NEO4j_URL, auth=(NEO4j_USER, NEO4j_PASSWORD,)).session(database=NEO4j_DATABASE)

    @classmethod
    def search_one(cls, query: Query, top: int) -> List[Tuple[int, float]]:
        result = cls.client.similarity_search_by_vector(query.vector, k=top, query='2')
        print(result)
        return [
            (uuid.UUID(hex=hit.page_content).int, 0.1)
            for hit in result
        ]

        # vector_search = "CALL db.index.vector.queryNodes($index, $k, $embedding) YIELD node, score "
        # default_retrieval = (
        #     f"RETURN node.`{NEO4j_INDEX}` AS text, score "
        # )

        # vector_search = """
        # WITH $embedding as e
        # call db.index.vector.queryNodes($index,$k,e) yield node,score
        # return node.id as result,score

        # finial = vector_search + default_retrieval
        #
        # res = cls.client.run(finial, {'embedding': vector, 'index': NEO4j_INDEX, 'k': 100})
        # a = [r.data() for r in res]
        # print(len(a))
        # knn = {
        #     "field": "vector",
        #     "query_vector": vector,
        #     "k": top,
        #     **{"num_candidates": 100, **cls.search_params},
        # }
        #
        # meta_conditions = cls.parser.parse(meta_conditions)
        # if meta_conditions:
        #     knn["filter"] = meta_conditions
        #
        # res = cls.client.search(
        #     index=ELASTIC_INDEX,
        #     knn=knn,
        #     size=top,
        # )
