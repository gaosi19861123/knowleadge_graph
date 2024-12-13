# Databricks notebook source
# -*- coding: utf-8 -*-
import src
from src.logger import KGLogger
from src.data import (
    read_pos_data, 
    get_target_pos_files, 
    DataTransformer,
    RelationshipExtractor
)

from src.config import Config
import pandas as pd

import importlib
importlib.reload(src.config)
importlib.reload(src.data)
logger = KGLogger("knowledge_graph")

# COMMAND ----------

#read config file and set parameters
cfg = Config()

target_file = (
    get_target_pos_files(
        campaign_start_date=cfg.campaign_start_date,
        tbl_sales_details=cfg.tbl_sales_fp,
        periods=cfg.query_period,
    )
)

logger.info(
    f"load_parameters:\
    {cfg.campaign_start_date}, {cfg.tbl_sales_fp}, {cfg.query_period}, {cfg.comp_cd}",
)

data = read_pos_data(
    spark, 
    file_path = target_file,  
    cols= cfg.pos_col,
    comp_cd=cfg.comp_cd,
)

logger.info(
    f"target_file: {target_file}, cols: {cfg.pos_col}, comp_cd: {cfg.comp_cd}"
)

display(
    data
)

# COMMAND ----------

def main():
    try:
        # 提取节点
        # 初始化转换器
        transformer = DataTransformer(spark)
        # 读取数据
        transformer.read_pyspark_dataframe(data)
        # 执行转换
        transformer.transform_all()
        # 保存结果
        transformer.save_to_json(cfg.node_fp)

        # 提取关系
        # 初始化提取器
        extractor = RelationshipExtractor(spark)
        # 提取关系
        purchases_df, visits_df = extractor.extract_relationships(data)
        # 保存关系数据
        extractor.save_relationships(purchases_df, visits_df, cfg.relation_fp)
        # 分析关系
        extractor.analyze_relationships(purchases_df, visits_df)

    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise

# COMMAND ----------

if __name__ == "__main__":
    main()
