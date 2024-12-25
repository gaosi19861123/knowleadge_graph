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
            WITH prod2, count(DISTINCT p) as frequency
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

    def get_user_based_recommendations(
        self, 
        person_id: str, 
        limit: int = 10,
        similarity_threshold: float = 0
    ) -> List[Dict[str, Any]]:
        """
        基于用户协同过滤的商品推荐
        
        Args:
            person_id: 用户ID
            limit: 返回推荐商品的最大数量
            similarity_threshold: 用户相似度阈值（0-1之间）
            
        Returns:
            推荐商品列表，每个商品包含相似度得分
        """
        try:
            query = """
            // 1. 获取目标用户的全部购买列表
            MATCH (target:Person {id: $person_id})-[:PURCHASE]->(p:Product)
            WITH target, collect(DISTINCT p.id) AS target_products

            // 2. 收集每个 "other" 用户的全部购买列表
            MATCH (other:Person)-[:PURCHASE]->(p2:Product)
            WHERE other <> target
            WITH target, other, target_products,
                collect(DISTINCT p2.id) AS other_products

            // 3. 计算相似度（使用原生Cypher计算Jaccard相似度）
            WITH target, other, target_products, other_products,
                [x IN target_products WHERE x IN other_products] AS common_products,
                size(target_products) AS target_size,
                size(other_products) AS other_size
            WITH target, other, target_products,
                CASE 
                    WHEN (target_size + other_size - size(common_products)) > 0
                    THEN toFloat(size(common_products)) / (target_size + other_size - size(common_products))
                    ELSE 0
                END AS similarity
            WHERE similarity >= $similarity_threshold

            // 4. 匹配 "相似用户" 购买的目标用户尚未购买的商品
            MATCH (other)-[:PURCHASE]->(rec_product:Product)
            WHERE NOT rec_product.id IN target_products

            // 5. 计算推荐得分，并收集相似用户信息
            WITH DISTINCT rec_product, other.id as similar_user_id, similarity
            WITH rec_product, 
                sum(similarity) AS total_similarity, 
                count(*) AS purchase_count,
                collect({user_id: similar_user_id, similarity: similarity}) as similar_users

            RETURN 
                rec_product.id AS product_id,
                rec_product.name AS product_name,
                rec_product.major_category AS category,
                total_similarity * log(purchase_count + 1) AS recommendation_score,
                purchase_count,
                similar_users
            ORDER BY recommendation_score DESC
            LIMIT $limit
            """
            
            with self.driver.session() as session:
                result = session.run(
                    query,
                    person_id=person_id,
                    similarity_threshold=similarity_threshold,
                    limit=limit
                )
                
                recommendations = [dict(record) for record in result]
                self.logger.info(
                    f"Generated {len(recommendations)} recommendations for user {person_id}"
                )
                return recommendations
                
        except Exception as e:
            self.logger.error(
                f"Error generating recommendations for user {person_id}: {str(e)}"
            )
            raise

    def get_item_based_recommendations(
        self, 
        person_id: str, 
        limit: int = 10,
        similarity_threshold: float = 0.1,
        max_history_items: int = 10
    ) -> List[Dict[str, Any]]:
        """
        基于物品的协同过滤推荐
        
        Args:
            person_id: 用户ID
            limit: 返回推荐商品的最大数量
            similarity_threshold: 物品相似度阈值（0-1之间）
            max_history_items: 用于推荐的最大历史购买商品数
            
        Returns:
            推荐商品列表，每个商品包含相似度得分
        """
        try:
            query = """
            // 获取用户最近购买的商品
            MATCH (target:Person {id: $person_id})-[r1:PURCHASE]->(history:Product)
            WITH target, history
            ORDER BY r1.datetime DESC
            LIMIT $max_history_items
            
            // 收集历史购买商品ID
            WITH target, collect(history) as history_products,
                 collect(history.id) as history_product_ids
            
            // 找到与历史购买商品有共同购买者的其他商品
            UNWIND history_products as history_prod
            MATCH (history_prod)<-[:PURCHASE]-(common_user:Person)-[:PURCHASE]->(rec_product:Product)
            WHERE NOT rec_product.id IN history_product_ids
            
            // 计算物品相似度
            WITH DISTINCT history_prod, rec_product,
                 // 共同购买者数量
                 count(common_user) as common_buyers,
                 // 历史商品的总购买者数量
                 size((history_prod)<-[:PURCHASE]-()) as history_buyers,
                 // 推荐商品的总购买者数量
                 size((rec_product)<-[:PURCHASE]-()) as rec_buyers
            
            // 使用Jaccard相似度
            WITH rec_product,
                 toFloat(common_buyers) / 
                 (history_buyers + rec_buyers - common_buyers) as similarity
            WHERE similarity >= $similarity_threshold
            
            // 聚合并计算最终推荐得分
            WITH rec_product,
                 sum(similarity) as similarity_score,
                 count(*) as connection_count
            
            RETURN 
                rec_product.id as product_id,
                rec_product.name as product_name,
                rec_product.major_category as category,
                similarity_score * log(connection_count + 1) as recommendation_score,
                connection_count as related_items_count
            ORDER BY recommendation_score DESC
            LIMIT $limit
            """
            
            with self.driver.session() as session:
                result = session.run(
                    query,
                    person_id=person_id,
                    similarity_threshold=similarity_threshold,
                    limit=limit,
                    max_history_items=max_history_items
                )
                
                recommendations = [dict(record) for record in result]
                self.logger.info(
                    f"Generated {len(recommendations)} item-based recommendations for user {person_id}"
                )
                return recommendations
                
        except Exception as e:
            self.logger.error(
                f"Error generating item-based recommendations for user {person_id}: {str(e)}"
            )
            raise

    def get_association_rules(
        self,
        min_support: float = 0.001,
        min_confidence: float = 0.1,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        获取商品关联规则
        
        Args:
            min_support: 最小支持度（0-1之间）
            min_confidence: 最小置信度（0-1之间）
            limit: 返回规则的最大数量
            
        Returns:
            关联规则列表，每个规则包含前项、后项、支持度、置信度和提升度
        """
        try:
            query = """
            // 计算总交易数
            MATCH (p:Person)
            WITH count(DISTINCT p) as total_transactions

            // 查找频繁项集
            MATCH (p:Person)-[:PURCHASE]->(prod1:Product)
            MATCH (p)-[:PURCHASE]->(prod2:Product)
            WHERE prod1.id < prod2.id  // 避免重复计算

            // 计算支持度和置信度
            WITH prod1, prod2, total_transactions,
                 count(DISTINCT p) as support_count,
                 size((prod1)<-[:PURCHASE]-()) as prod1_count,
                 size((prod2)<-[:PURCHASE]-()) as prod2_count
            
            WITH prod1, prod2,
                 toFloat(support_count) / total_transactions as support,
                 toFloat(support_count) / prod1_count as conf1_2,
                 toFloat(support_count) / prod2_count as conf2_1,
                 toFloat(prod1_count) / total_transactions as prod1_support,
                 toFloat(prod2_count) / total_transactions as prod2_support,
                 support_count, total_transactions
            
            WHERE support >= $min_support AND 
                  (conf1_2 >= $min_confidence OR conf2_1 >= $min_confidence)
            
            // 计算提升度
            WITH prod1, prod2, support, conf1_2, conf2_1,
                 prod1_support, prod2_support,
                 conf1_2 / prod2_support as lift1_2,
                 conf2_1 / prod1_support as lift2_1
            
            // 返回两个方向的规则
            RETURN 
                prod1.id as antecedent_id,
                prod1.name as antecedent_name,
                prod2.id as consequent_id,
                prod2.name as consequent_name,
                support as support,
                conf1_2 as confidence,
                lift1_2 as lift
            ORDER BY lift DESC
            LIMIT $limit
            """
            
            with self.driver.session() as session:
                result = session.run(
                    query,
                    min_support=min_support,
                    min_confidence=min_confidence,
                    limit=limit
                )
                
                rules = [dict(record) for record in result]
                self.logger.info(f"Generated {len(rules)} association rules")
                return rules
                
        except Exception as e:
            self.logger.error(f"Error generating association rules: {str(e)}")
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
        person_id="1000000101426205",
        limit=5,
        start_date="2024-12-01",
        end_date="2024-12-11"
    )
    print(purchases)

    # 获取商品的购买者
    purchasers = querier.get_product_purchasers(
        product_id="4902519045922",
        limit=5,
        start_date="2024-01-01",  # 添加开始日期
        end_date="2024-12-31", # 添加结束日期
    )
    print(purchasers)

    #get_related_products
    related_products = querier.get_related_products(
        product_id="4902519045922",
        limit=5
    )
    print(related_products)

    # 基于用户协同过滤的推荐
    # recommendations = querier.get_user_based_recommendations(
    #     person_id="1000000195718338",
    #     limit=5,
    #     similarity_threshold=0
    # )
    # print("推荐商品：")
    # for rec in recommendations:
    #     print(f"商品ID: {rec['product_id']}")
    #     print(f"商品名称: {rec['product_name']}")
    #     print(f"分类: {rec['category']}")
    #     print(f"推荐得分: {rec['recommendation_score']:.3f}")
    #     print(f"购买次数: {rec['purchase_count']}")
    #     print("相似用户：")
    #     for user in rec['similar_users']:
    #         print(f"  - 用户ID: {user['user_id']}, 相似度: {user['similarity']:.3f}")
    #     print("---")

    # # 基于物品的协同过滤推荐
    # item_recommendations = querier.get_item_based_recommendations(
    #     person_id="1000000195718338",
    #     limit=5,
    #     similarity_threshold=0.0,
    #     max_history_items=10
    # )
    # print("\n基于物品的推荐商品：")
    # for rec in item_recommendations:
    #     print(f"商品ID: {rec['product_id']}")
    #     print(f"商品名称: {rec['product_name']}")
    #     print(f"分类: {rec['category']}")
    #     print(f"推荐得分: {rec['recommendation_score']:.3f}")
    #     print(f"相关商品数: {rec['related_items_count']}")
    #     print("---")

    
    # # 在main部分添加测试代码
    # association_rules = querier.get_association_rules(
    #     min_support=0.001,
    #     min_confidence=0.1,
    #     limit=5
    # )
    # print("\n商品关联规则：")
    # for rule in association_rules:
    #     print(f"规则：{rule['antecedent_name']} -> {rule['consequent_name']}")
    #     print(f"支持度: {rule['support']:.4f}")
    #     print(f"置信度: {rule['confidence']:.4f}")
    #     print(f"提升度: {rule['lift']:.4f}")
    #     print("---")
    
    # 关闭连接
    querier.close()