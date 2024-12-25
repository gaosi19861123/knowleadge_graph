from neo4j import GraphDatabase
import logging
from typing import List, Dict, Any
from datetime import datetime
from dotenv import load_dotenv
import os 

class Neo4jQuerier:
    def __init__(self, uri: str, user: str, password: str):
        """
        初始化Neo4j查询器
        
        Args:
            uri: Neo4j数据库URI
            user: 用户名
            password: 密码
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.logger = self._setup_logger()
    
    @staticmethod
    def _setup_logger():
        """设置日志记录器"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)

    def close(self):
        """关闭数据库连接"""
        self.driver.close()

    def get_person_purchases(
        self, 
        person_id: str, 
        limit: int = 10, 
        start_date: str = None, 
        end_date: str = None
    ) -> List[Dict[str, Any]]:
        """
        获取指定用户的购买记录
        
        Args:
            person_id: 用户ID
            limit: 返回记录的最大数量
            start_date: 开始日期 (YYYY-MM-DD格式)
            end_date: 结束日期 (YYYY-MM-DD格式)
            
        Returns:
            包含购买记录的列表
        """
        try:
            # 日付文字列を変換
            if start_date:
                formatted_start = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y%m%dT000000")
            if end_date:
                formatted_end = datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y%m%dT235959")

            query = """
            MATCH (p:Person {id: $person_id})-[r:PURCHASE]->(prod:Product)
            WHERE 
                ($start_date IS NULL OR r.datetime >= $start_date) AND
                ($end_date IS NULL OR r.datetime <= $end_date)
            RETURN 
                prod.id as product_id,
                prod.name as product_name,
                r.datetime as purchase_date,
                r.quantity as quantity,
                r.amount as amount,
                r.payment_method as payment_method,
                r.store_id as store_id
            ORDER BY r.datetime DESC
            LIMIT $limit
            """
            
            with self.driver.session() as session:
                result = session.run(
                    query,
                    person_id=person_id,
                    limit=limit,
                    start_date=formatted_start if start_date else None,
                    end_date=formatted_end if end_date else None
                )
                
                purchases = [dict(record) for record in result]
                self.logger.info(f"Retrieved {len(purchases)} purchases for person {person_id}")
                return purchases
                
        except Exception as e:
            self.logger.error(f"Error retrieving purchases for person {person_id}: {str(e)}")
            raise

    def get_product_purchasers(
        self, 
        product_id: str, 
        limit: int = 10,
        start_date: str = None,
        end_date: str = None
    ) -> List[Dict[str, Any]]:
        """
        获取购买指定商品的用户记录
        
        Args:
            product_id: 商品ID
            limit: 返回记录的最大数量
            start_date: 开始日期 (YYYY-MM-DD格式)
            end_date: 结束日期 (YYYY-MM-DD格式)
            
        Returns:
            包含购买用户记录的列表
        """
        try:
            # 日付文字列を変換
            if start_date:
                formatted_start = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y%m%dT000000")
            if end_date:
                formatted_end = datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y%m%dT235959")

            query = """
            MATCH (p:Person)-[r:PURCHASE]->(prod:Product {id: $product_id})
            WHERE 
                ($start_date IS NULL OR r.datetime >= $start_date) AND
                ($end_date IS NULL OR r.datetime <= $end_date)
            RETURN 
                p.id as person_id,
                p.gender as gender,
                p.prefecture as prefecture,
                r.datetime as purchase_date,
                r.quantity as quantity,
                r.amount as amount,
                r.store_id as store_id
            ORDER BY r.datetime DESC
            LIMIT $limit
            """
            
            with self.driver.session() as session:
                result = session.run(
                    query,
                    product_id=product_id,
                    limit=limit,
                    start_date=formatted_start if start_date else None,
                    end_date=formatted_end if end_date else None
                )
                
                purchasers = [dict(record) for record in result]
                self.logger.info(f"Retrieved {len(purchasers)} purchasers for product {product_id}")
                return purchasers
                
        except Exception as e:
            self.logger.error(f"Error retrieving purchasers for product {product_id}: {str(e)}")
            raise

    def get_purchase_statistics(
        self, 
        entity_id: str, 
        entity_type: str = "person"
    ) -> Dict[str, Any]:
        """
        获取购买统计信息
        
        Args:
            entity_id: 实体ID（人员ID或商品ID）
            entity_type: 实体类型 ("person" 或 "product")
            
        Returns:
            包含统计信息的字典
        """
        try:
            if entity_type == "person":
                query = """
                MATCH (p:Person {id: $entity_id})-[r:PURCHASE]->(prod:Product)
                RETURN 
                    count(r) as total_purchases,
                    sum(r.quantity) as total_quantity,
                    sum(r.amount) as total_amount,
                    avg(r.amount) as avg_amount,
                    collect(DISTINCT prod.major_category) as categories
                """
            else:  # product
                query = """
                MATCH (p:Person)-[r:PURCHASE]->(prod:Product {id: $entity_id})
                RETURN 
                    count(r) as total_purchases,
                    sum(r.quantity) as total_quantity,
                    sum(r.amount) as total_amount,
                    avg(r.amount) as avg_amount,
                    count(DISTINCT p.id) as unique_customers
                """
            
            with self.driver.session() as session:
                result = session.run(query, entity_id=entity_id)
                stats = dict(result.single())
                self.logger.info(f"Retrieved statistics for {entity_type} {entity_id}")
                return stats
                
        except Exception as e:
            self.logger.error(f"Error retrieving statistics for {entity_type} {entity_id}: {str(e)}")
            raise

    def get_related_products(self, product_id: str, limit: int = 5) -> List[Dict[str, Any]]:
        """
        获取与指定商品相关的其他商品（经常一起购买的商品）
        
        Args:
            product_id: 商品ID
            limit: 返回记录的最大数量
            
        Returns:
            相关商品列表
        """
        try:
            query = """
            MATCH (prod1:Product {id: $product_id})<-[:PURCHASE]-(p:Person)-[:PURCHASE]->(prod2:Product)
            WHERE prod1 <> prod2
            WITH prod2, count(*) as frequency
            RETURN 
                prod2.id as product_id,
                prod2.name as product_name,
                prod2.major_category as category,
                frequency
            ORDER BY frequency DESC
            LIMIT $limit
            """
            
            with self.driver.session() as session:
                result = session.run(query, product_id=product_id, limit=limit)
                related_products = [dict(record) for record in result]
                self.logger.info(f"Retrieved {len(related_products)} related products for product {product_id}")
                return related_products
                
        except Exception as e:
            self.logger.error(f"Error retrieving related products for {product_id}: {str(e)}")
            raise 

if __name__ == "__main__":
    # 初始化查询器
    load_dotenv()
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER")
    password = os.getenv("NEO4J_PASSWORD")
    querier = Neo4jQuerier(uri, user, password)

    # 获取用户购买记录
    purchases = querier.get_person_purchases(
        person_id="1000000195648492",
        limit=5,
        start_date="2024-12-01",
        end_date="2024-12-11"
    )
    print(purchases)


    # 获取商品的购买者
    purchasers = querier.get_product_purchasers(
        product_id="2311546305039",
        limit=5
    )
    print(purchasers)

    # 获取购买统计
    # stats = querier.get_purchase_statistics(
    #     entity_id="12345",
    #     entity_type="person"
    # )
    # print(stats)

#     # # 获取相关商品
#     # related = querier.get_related_products(
#     #     product_id="67890",
#     #     limit=5
#     # )

#     # 关闭连接
#     querier.close()