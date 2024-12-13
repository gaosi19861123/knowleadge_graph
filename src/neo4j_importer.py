from neo4j import GraphDatabase
import json
import logging
from typing import Dict, List
from datetime import datetime

class Neo4jImporter:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.logger = self._setup_logger()
    
    @staticmethod
    def _setup_logger():
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)

    def close(self):
        self.driver.close()

    def clear_database(self):
        """清空数据库"""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            self.logger.info("Database cleared")

    def create_constraints(self):
        """创建唯一性约束"""
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE p.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Product) REQUIRE p.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Store) REQUIRE s.id IS UNIQUE"
        ]
        
        with self.driver.session() as session:
            for constraint in constraints:
                session.run(constraint)
        self.logger.info("Constraints created")

    def import_persons(self, file_path: str):
        """导入Person节点"""
        query = """
        MERGE (p:Person {id: $id})
        SET p.gender = $gender,
            p.birth_date = $birth_date,
            p.location = $location,
            p.contact_preferences = $contact_preferences,
            p.membership = $membership
        """
        
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)['persons']
            with self.driver.session() as session:
                count = 0
                for person in data:
                    session.run(query, person)
                    count += 1
                self.logger.info(f"Imported {count} person nodes")

    def import_products(self, file_path: str):
        """导入Product节点"""
        query = """
        MERGE (p:Product {id: $id})
        SET p.name = $name,
            p.name_kana = $name_kana,
            p.categories = $categories,
            p.pricing = $pricing
        """
        
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)['products']
            with self.driver.session() as session:
                count = 0
                for product in data:
                    session.run(query, product)
                    count += 1
                self.logger.info(f"Imported {count} product nodes")

    def import_stores(self, file_path: str):
        """导入Store节点"""
        query = """
        MERGE (s:Store {id: $id})
        SET s.company_id = $company_id,
            s.register_numbers = $register_numbers
        """
        
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)['stores']
            with self.driver.session() as session:
                count = 0
                for store in data:
                    session.run(query, store)
                    count += 1
                self.logger.info(f"Imported {count} store nodes")

    def import_purchase_relationships(self, file_path: str):
        """导入购买关系"""
        query = """
        MATCH (p:Person {id: $person_id})
        MATCH (prod:Product {id: $product_id})
        MERGE (p)-[r:PURCHASE {id: $id}]->(prod)
        SET r.shop_id = $shop_id,
            r.register_no = $register_no,
            r.sales_date = $sales_date,
            r.sales_time = $sales_time,
            r.transaction_details = $transaction_details
        """
        
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)['purchases']
            with self.driver.session() as session:
                count = 0
                for purchase in data:
                    session.run(query, purchase)
                    count += 1
                self.logger.info(f"Imported {count} purchase relationships")

    def import_visit_relationships(self, file_path: str):
        """导入访问关系"""
        query = """
        MATCH (p:Person {id: $person_id})
        MATCH (s:Store {id: $store_id})
        MERGE (p)-[r:VISIT {id: $id}]->(s)
        SET r.datetime = datetime($datetime),
            r.visit_details = $visit_details
        """
        
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)['visits']
            with self.driver.session() as session:
                count = 0
                for visit in data:
                    session.run(query, visit)
                    count += 1
                self.logger.info(f"Imported {count} visit relationships")

    def verify_import(self):
        """验证导入的数据"""
        verification_queries = {
            "Person nodes": "MATCH (p:Person) RETURN count(p) as count",
            "Product nodes": "MATCH (p:Product) RETURN count(p) as count",
            "Store nodes": "MATCH (s:Store) RETURN count(s) as count",
            "Purchase relationships": "MATCH ()-[r:PURCHASE]->() RETURN count(r) as count",
            "Visit relationships": "MATCH ()-[r:VISIT]->() RETURN count(r) as count"
        }
        
        with self.driver.session() as session:
            for description, query in verification_queries.items():
                result = session.run(query).single()
                self.logger.info(f"{description}: {result['count']}")