from src.neo4j_importer import Neo4jImporter
from dotenv import load_dotenv
from src.logger import KGLogger
import os 

def main():
    logger = KGLogger("knowledge_graph")
    logger.info("Start importing knowledge graph")
    load_dotenv()
    neo4j_user = os.getenv('NEO4J_USER')
    neo4j_password = os.getenv('NEO4J_PASSWORD')
    neo4j_uri = os.getenv('NEO4J_URI')
    neo4j_importer = Neo4jImporter(neo4j_uri, neo4j_user, neo4j_password)
    # neo4j_importer.clear_database()
    # neo4j_importer.close()
    logger.info("End importing knowledge graph")
if __name__ == "__main__":
    main()
