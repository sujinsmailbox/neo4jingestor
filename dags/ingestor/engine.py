from neo4j import Neo4jIngestor

class CreateIngestor:
    def __int__(self, file_type, file_source, db_type):
        self.file_type = file_type
        self.file_source = file_source
        self.db_type = db_type


    def create_engine(self):
        if self.file_type == 'xml' and self.file_source == 'queue' and self.db_type == "neo4j":
            return Neo4jIngestor(file_type = 'xml')
        else:
            pass


if __name__ == "__main__":
    CreateIngestor()
