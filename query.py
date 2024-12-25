from src.neo4j_query import Neo4jQuerier
import os
from dotenv import load_dotenv

if __name__ == "__main__":
    load_dotenv()
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER")
    password = os.getenv("NEO4J_PASSWORD")
    querier = Neo4jQuerier(uri, user, password)

    # purchases = querier.get_person_purchases(
    #     person_id="12345",
    #     limit=5,
    #     start_date="2024-01-01",
    #     end_date="2024-03-31"
    # )
    # print(purchases)