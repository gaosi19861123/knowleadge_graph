from neo4j import GraphDatabase
import json
import logging
import os
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

    def import_persons(self, directory_path: str, batch_mode: bool = True):
        """
        导入Person节点数据
        Args:
            directory_path: 包含JSON文件的目录路径
            batch_mode: 是否使用批量处理模式
        """
        if not os.path.isdir(directory_path):
            raise ValueError(f"目录不存在: {directory_path}")

        # 获取目录下所有JSON文件
        json_files = sorted([f for f in os.listdir(directory_path) if f.endswith('.json')])
        total_files = len(json_files)
        self.logger.info(f"找到 {total_files} 个JSON文件")

        batch_size = 1000
        total_records = 0
        batch = []

        try:
            for file_idx, json_file in enumerate(json_files, 1):
                file_path = os.path.join(directory_path, json_file)
                self.logger.info(f"处理文件 {file_idx}/{total_files}: {json_file}")

                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        if not line.strip():
                            continue

                        try:
                            person = json.loads(line.strip())
                            
                            # 构建person数据
                            person_data = {
                                'id': person['id'],
                                'gender': person.get('gender', '0'),
                                'birth_date': person.get('birth_date', ''),
                                'location': {
                                    'prefecture': person.get('location', {}).get('prefecture', ''),
                                    'zip_code': person.get('location', {}).get('zip_code', ''),
                                    'coordinates': {
                                        'latitude': person.get('location', {}).get('coordinates', {}).get('latitude', ''),
                                        'longitude': person.get('location', {}).get('coordinates', {}).get('longitude', '')
                                    }
                                },
                                'membership': {
                                    'member_id': person.get('membership', {}).get('member_id', ''),
                                    'status': person.get('membership', {}).get('status', '')
                                }
                            }

                            batch.append(person_data)
                            
                            # 批量处理
                            if len(batch) >= batch_size:
                                self._batch_import_persons(batch)
                                total_records += len(batch)
                                self.logger.info(f"已处理 {total_records} 条记录")
                                batch = []

                        except json.JSONDecodeError as e:
                            self.logger.error(f"JSON解析错误: {str(e)}")
                            continue

                # 处理文件末尾的剩余批次
                if batch:
                    self._batch_import_persons(batch)
                    total_records += len(batch)
                    batch = []

                self.logger.info(f"完成文件处理: {json_file}")

            self.logger.info(f"所有文件处理完成，共导入 {total_records} 条记录")

        except Exception as e:
            self.logger.error(f"导入过程中发生错误: {str(e)}")
            raise

    def _batch_import_persons(self, batch):
        """
        批量导入人员数据到Neo4j
        """
        query = """
        UNWIND $batch AS person
        MERGE (p:Person {id: person.id})
        SET p.gender = person.gender,
            p.birth_date = person.birth_date,
            p.prefecture = person.location.prefecture,
            p.zip_code = person.location.zip_code,
            p.latitude = person.location.coordinates.latitude,
            p.longitude = person.location.coordinates.longitude,
            p.member_id = person.membership.member_id,
            p.status = person.membership.status
        """
        
        try:
            with self.driver.session() as session:
                session.run(query, batch=batch)
        except Exception as e:
            self.logger.error(f"批量导入执行错误: {str(e)}")
            raise

    def import_products(self, directory_path: str):
        """
        导入Product节点数据
        Args:
            directory_path: JSON文件路径
        """

        if not os.path.isdir(directory_path):
            raise ValueError(f"目录不存在: {directory_path}")

        # 获取目录下所有JSON文件
        json_files = sorted([f for f in os.listdir(directory_path) if f.endswith('.json')])
        total_files = len(json_files)
        self.logger.info(f"开始导入产品数据找到 {total_files} 个JSON文件")
    
        batch_size = 1000
        total_records = 0
        batch = []

        try:
            for file_idx, json_file in enumerate(json_files, 1):
                file_path = os.path.join(directory_path, json_file)
                self.logger.info(f"处理文件 {file_idx}/{total_files}: {json_file}")
                
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        if not line.strip():
                            continue

                        try:
                            product = json.loads(line.strip())
                            
                            # 处理categories数据，将嵌套结构展平
                            categories = product.get('categories', {})
                            product_data = {
                                'id': product['id'],
                                'name': product.get('name', ''),
                                'name_kana': product.get('name_kana', ''),
                                # 展平categories结构
                                'major_category': categories.get('major_category', ''),
                                'medium_category': categories.get('medium_category', ''),
                                'minor_category': categories.get('minor_category', ''),
                                'group_code': categories.get('group', {}).get('code', ''),
                                'group_name': categories.get('group', {}).get('name', ''),
                                'department_code': categories.get('department', {}).get('code', ''),
                                'department_name': categories.get('department', {}).get('name', ''),
                                # 处理pricing数据
                                'price': product.get('pricing', {}).get('price', 0),
                                'tax_rate': product.get('pricing', {}).get('tax_rate', 0)
                            }

                            batch.append(product_data)
                            
                            if len(batch) >= batch_size:
                                self._batch_import_products(batch)
                                total_records += len(batch)
                                self.logger.info(f"已处理 {total_records} 条记录")
                                batch = []

                        except json.JSONDecodeError as e:
                            self.logger.error(f"JSON解析错误: {str(e)}")
                            continue

                    # 处理剩余的批次
                    if batch:
                        self._batch_import_products(batch)
                        total_records += len(batch)

                self.logger.info(f"产品数据导入完成，共导入 {total_records} 条记录")

        except Exception as e:
            self.logger.error(f"导入过程中发生错误: {str(e)}")
            raise

    def _batch_import_products(self, batch):
        """
        批量导入产品数据到Neo4j
        Args:
            batch: 包含产品数据的列表
        """
        query = """
        UNWIND $batch AS product
        MERGE (p:Product {id: product.id})
        SET p.name = product.name,
            p.name_kana = product.name_kana,
            p.major_category = product.major_category,
            p.medium_category = product.medium_category,
            p.minor_category = product.minor_category,
            p.group_code = product.group_code,
            p.group_name = product.group_name,
            p.department_code = product.department_code,
            p.department_name = product.department_name,
            p.price = product.price,
            p.tax_rate = product.tax_rate
        """
        
        try:
            with self.driver.session() as session:
                session.run(query, batch=batch)
        except Exception as e:
            self.logger.error(f"批量导入执行错误: {str(e)}")
            raise

    def import_stores(self, directory_path: str):
        """
        导入Store节点数据
        Args:
            directory_path: 包含JSON文件的目录路径
        """
        if not os.path.isdir(directory_path):
            raise ValueError(f"目录不存在: {directory_path}")

        # 获取目录下所有JSON文件
        json_files = sorted([f for f in os.listdir(directory_path) if f.endswith('.json')])
        total_files = len(json_files)
        self.logger.info(f"找到 {total_files} 个JSON文件")

        batch_size = 1000
        total_records = 0
        batch = []

        try:
            for file_idx, json_file in enumerate(json_files, 1):
                file_path = os.path.join(directory_path, json_file)
                self.logger.info(f"处理文件 {file_idx}/{total_files}: {json_file}")

                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        if not line.strip():
                            continue

                        try:
                            store = json.loads(line.strip())
                            batch.append(store)
                            
                            # 批量处理
                            if len(batch) >= batch_size:
                                self._batch_import_stores(batch)
                                total_records += len(batch)
                                self.logger.info(f"已处理 {total_records} 条记录")
                                batch = []

                        except json.JSONDecodeError as e:
                            self.logger.error(f"JSON解析错误: {str(e)}")
                            continue

                # 处理文件末尾的剩余批次
                if batch:
                    self._batch_import_stores(batch)
                    total_records += len(batch)
                    batch = []

                self.logger.info(f"完成文件处理: {json_file}")

            self.logger.info(f"所有商店数据导入完成，共导入 {total_records} 条记录")

        except Exception as e:
            self.logger.error(f"导入过程中发生错误: {str(e)}")
            raise

    def _batch_import_stores(self, batch):
        """
        批量导入商店数据到Neo4j
        Args:
            batch: 包含商店数据的列表
        """
        query = """
        UNWIND $batch AS store
        MERGE (s:Store {id: store.id})
        SET s.company_id = store.company_id
        """
        
        try:
            with self.driver.session() as session:
                session.run(query, batch=batch)
        except Exception as e:
            self.logger.error(f"批量导入执行错误: {str(e)}")
            raise

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