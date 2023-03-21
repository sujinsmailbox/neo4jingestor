import json
import logging
import xml.etree.ElementTree as ET

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
from xmlschema import XMLSchema

logging.basicConfig(level='INFO', format='%(asctime)s | %(levelname)s | %(message)s')


class Neo4jWritter:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def check_connection(self):
        try:
            with self.driver.session(database="neo4j") as session:
                session.close()
        except Exception:
            logging.exception('Unable to open a session')

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    def create_relation(self, element_tag, element_attrib, prev_element_attrib):
        with self.driver.session(database="neo4j") as session:
            # Write transactions allow the driver to handle retries and transient errors

            if 'name' not in prev_element_attrib.keys():
                session.execute_write(self._create_root, element_tag, element_attrib)
                logging.info(f"Written root node {element_tag}")
            else:
                prev_tag = prev_element_attrib['name']
                session.execute_write(self._create_node_and_relate, element_tag, element_attrib, prev_tag, prev_element_attrib)
                logging.info(f"Written node {element_tag} and mapped to {prev_tag}")

    @staticmethod
    def _create_node_and_relate(tx, element_tag, element_attrib, prev_tag, prev_element_attrib, *args):
        # To learn more about the Cypher syntax, see https://neo4j.com/docs/cypher-manual/current/
        # The Reference Card is also a good resource for keywords https://neo4j.com/docs/cypher-refcard/current/

        node_props = "{" + ", ".join([f"{key}: '{value}'" for key, value in element_attrib.items()]) + "}"
        pre_node_props = "{" + ", ".join([f"{key}: '{value}'" for key, value in prev_element_attrib.items()]) + "}"

        query = (
            f"MATCH (parent_elem:{prev_tag} {pre_node_props}) "
            f"CREATE (current_elem:{element_tag} {node_props}) "
            f"CREATE (parent_elem)-[:{prev_tag}]->(current_elem) "
        )
        try:
            # logging.info(query)
            tx.run(query)
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    @staticmethod
    def _create_root(tx, root_node, element_attrib):
        # To learn more about the Cypher syntax, see https://neo4j.com/docs/cypher-manual/current/
        # The Reference Card is also a good resource for keywords https://neo4j.com/docs/cypher-refcard/current/
        node_props = "{" + ", ".join([f"{key}: '{value}'" for key, value in element_attrib.items()]) + "}"

        query = (
            f"CREATE (:{root_node} {node_props})"
        )
        # logging.info(query)
        try:
            tx.run(query)
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise


def _get_config_file(config_file_type):
    if config_file_type == 'uniprot':
        return '/usr/local/airflow/dags/config/uniprot.xsd'
    else:
        return ''

def _is_valid_xml(input_file, config_file_type):
    schema_file = _get_config_file(config_file_type)
    schema = XMLSchema(schema_file)
    tree = ET.parse(input_file)
    root = tree.getroot()
    if schema.is_valid(root):
        logging.info('XML is valid against the XSD schema')
        return True
    else:
        logging.info('XML is NOT valid against the XSD schema')
        return False

def _create_params(element, prev_element_dict:dict()):
    element_tag = element.tag.replace('{http://uniprot.org/uniprot}', '')
    element_dict = element.attrib
    element_dict['name'] = element_tag

    if 'node_index' not in prev_element_dict.keys():
        element_dict.clear()
        element_dict['name'] = element_tag
        element_dict['node_index'] = 1
    else:
        element_dict['node_index'] = prev_element_dict['node_index'] + 1

    if 'text' in element_dict and element_dict['text'] != '':
        element_dict['text'] = element.text.replace("'", '')
    return element_tag, element_dict


def _write_json(data_dict):
    json_data = json.dumps(data_dict)
    with open("data.json", "w") as json_file:
        json_file.write(json_data)

class Neo4jIngestor:

    def __init__(self, queue_name, queue_type, conn):
        self.queue_name = queue_name
        self.neo4j_conn = conn
        self.queue_type = queue_type
        self.config_file_type = 'uniprot'
        self.ingestion_status = 'initiated'

    def check_connection(self):
        logging.info(self.neo4j_conn)
        writer = Neo4jWritter(self.neo4j_conn['uri'], self.neo4j_conn['user'], self.neo4j_conn['password'])
        writer.check_connection()


    def ingest_data(self):
        self.ingestion_status = 'running'
        for item in self.queue_name:
            if not _is_valid_xml(item, self.config_file_type):
                return
            self._process_data(item)
        self.ingestion_status = 'stopped'

    def _write_to_database(self, *args):
        writer = Neo4jWritter(self.neo4j_conn['uri'], self.neo4j_conn['user'], self.neo4j_conn['password'])
        writer.create_relation(*args)
        writer.close()

    def _process_data(self, input_file):
        tree = ET.parse(input_file)
        root = tree.getroot()
        self._create_nodes(root, prev_element_dict=dict())

    def _create_nodes(self, element, prev_element_dict):

        element_tag, element_dict = _create_params(element, prev_element_dict)

        self._write_to_database(element_tag, element_dict, prev_element_dict)
        for child in element:
            self._create_nodes(child, element_dict)

    @property
    def ingestion_status(self):
        return self._ingestion_status

    @ingestion_status.setter
    def ingestion_status(self, value):
        self._ingestion_status = value

    @ingestion_status.deleter
    def ingestion_status(self):  # again, the method name is the same
        del self._ingestion_status

if __name__ == "__main__":
    pass
