import logging
from typing import Any
import time
import json
import ast

from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.utils.decorators import apply_defaults

from ingestor.neo4j import Neo4jIngestor


class IngestIntoNeo4jOperator(BaseOperator):

    template_fields = ['queue_name']
    @apply_defaults
    def __init__(self,
                 *,
                 queue_name,
                 queue_type=None,  # placeholder for incorporating queiung system
                 neo4j_conn,
                 **kwargs
                 ):
        super().__init__(**kwargs)
        self.queue_name = queue_name
        self.queue_type = 'xcom'
        self.neo4j_conn = neo4j_conn

    def execute(self, context: Context) -> Any:
        logging.info("printing input params")
        logging.info(self.queue_name)
        logging.info(type(self.queue_name))

        self.queue_name = ast.literal_eval(self.queue_name)

        logging.info(self.queue_name)
        logging.info(type(self.queue_name))

        self.neo4j_conn = {'uri':'neo4j://172.21.0.3:7687', 'user':'neo4j', 'password': 'adminadmin'}

        logging.info(self.neo4j_conn)

        ingestor = Neo4jIngestor(self.queue_name, self.queue_type, self.neo4j_conn)
        ingestor.check_connection()
        if self.queue_type == 'xcom':
            ingestor.ingest_data()

        while True:
            if ingestor.ingestion_status != 'stopped':
                time.sleep(1000)
            else:
                return
