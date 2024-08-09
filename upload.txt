import multiprocessing as mp
from typing import List, Optional

import neo4j

from dataset_reader.base_reader import Record
from engine.base_client.upload import BaseUploader
from engine.clients.neo4j.config import NEO4j_URL, NEO4j_USER, NEO4j_PASSWORD, NEO4j_DATABASE, NEO4j_INDEX


class Neo4jUploader(BaseUploader):
    client: neo4j.Session = None
    upload_params = {}

    @classmethod
    def get_mp_start_method(cls):
        return "forkserver" if "forkserver" in mp.get_all_start_methods() else "spawn"

    @classmethod
    def init_client(cls, host, distance, connection_params, upload_params):
        cls.client = neo4j.GraphDatabase. \
            driver(NEO4j_URL, auth=(NEO4j_USER, NEO4j_PASSWORD,)).session(database=NEO4j_DATABASE)

    @classmethod
    def upload_batch(cls, batch: List[Record]):
        chunks = {
            "data": [
                {'text': record.id, 'embedding': record.vector}
                for record in batch
            ]
        }
        import_str = """
        UNWIND $data AS row
        CALL { WITH row 
        MERGE ( c:""" + NEO4j_INDEX + """ {text: row.text})
        with c , row
        CALL db.create.setVectorProperty(c,'embedding',row.embedding)
        YIELD node
        SET c.text = row.text} IN TRANSACTIONS OF 1000 ROWS
        """
        cls.client.run(import_str, chunks)

    @classmethod
    def post_upload(cls, _distance):
        cls.client.close()
        return {}
