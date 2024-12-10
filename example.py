from src.logger import KGLogger

# 创建logger实例
logger = KGLogger("knowledge_graph")

# 使用logger
logger.info("开始构建知识图谱")
logger.warning("发现重复节点")
logger.error("连接数据库失败")