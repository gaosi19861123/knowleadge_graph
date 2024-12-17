from src.neo4j_importer import Neo4jImporter
from dotenv import load_dotenv
from src.logger import KGLogger
import os 
import json

def main():
    logger = KGLogger("knowledge_graph")
    logger.info("Start importing knowledge graph")
    load_dotenv()
    neo4j_user = os.getenv('NEO4J_USER')
    neo4j_password = os.getenv('NEO4J_PASSWORD')
    neo4j_uri = os.getenv('NEO4J_URI')
    
    # 初始化导入器
    importer = Neo4jImporter(neo4j_uri, neo4j_user, neo4j_password)
     # JSON文件路径配置
    json_files = {
        "persons": os.getcwd() + "/data/node/persons.json",
        "products": os.getcwd() + "/data/node/products.json",
        "stores": os.getcwd() + "/data/node/stores.json",
        "purchases": os.getcwd() + "/data/relation/purchases.json",
        "visits": os.getcwd() + "/data/relation/visits.json"
    }

    try:        
        # 清空数据库并创建约束
        importer.clear_database()
        importer.create_constraints()
        
        # 导入节点
        importer.import_persons(json_files["persons"])
        importer.import_products(json_files["products"])
        importer.import_stores(json_files["stores"])
        
        # # 导入关系
        importer.import_purchase_relationships(json_files["purchases"])
        importer.import_visit_relationships(json_files["visits"])
        
        # 验证导入结果
        importer.verify_import()
        
    except Exception as e:
        logger.error(f"Error during import: {str(e)}")
        raise

    finally:
        if 'importer' in locals():
            importer.close()

    logger.info("End importing knowledge graph")

if __name__ == "__main__":
    main()
