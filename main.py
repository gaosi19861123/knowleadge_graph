# Databricks notebook source
# -*- coding: utf-8 -*-
import src
from src.logger import KGLogger
from src.data import (
    read_pos_data, 
    get_target_pos_files, 
    DataTransformer
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

logger.info(f"target_file: {target_file}, cols: {cfg.pos_col}, comp_cd: {cfg.comp_cd}")

# COMMAND ----------

def main():
    try:
        # 初始化转换器
        transformer = DataTransformer(spark)

        # 读取数据
        transformer.read_pyspark_dataframe(data)

        # 执行转换
        transformer.transform_all()

        # 保存结果
        transformer.save_to_json(cfg.node_fp)

    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise

# COMMAND ----------

if __name__ == "__main__":
    main()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as fn
from pyspark.sql.types import *
import logging
from typing import Dict, Optional

class RelationshipExtractor:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = self._setup_logger()
        
    @staticmethod
    def _setup_logger():
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)

    def extract_relationships(self, df):
        """提取购买关系和访问关系"""
        try:
            # 创建购买关系
            purchases_df = self._create_purchase_relationships(df)
            self.logger.info(f"Created {purchases_df.count()} purchase relationships")

            # 创建访问关系
            visits_df = self._create_visit_relationships(df)
            self.logger.info(f"Created {visits_df.count()} visit relationships")

            return purchases_df, visits_df

        except Exception as e:
            self.logger.error(f"Error extracting relationships: {str(e)}")
            raise

    def _create_purchase_relationships(self, df):
        """创建购买关系数据"""
        return df.select(
            # 基本信息
            fn.concat_ws('_', 
                fn.col('RECEIPT_NO'), 
                fn.col('SALES_DETAIL_NO')
            ).alias('id'),
            fn.col('CUSTOMER_ID').alias('person_id'),
            fn.col('ITEM_CD_UNIQUE').alias('product_id'),
            
            # 时间信息
            fn.concat(
                fn.col('SALES_YMD').cast('string'),
                fn.lit('T'),
                fn.col('SALES_HMS')
            ).alias('datetime'),
            
            # 购买详情
            fn.struct(
                fn.col('SALES_NUM').cast('decimal(18,3)').alias('quantity'),
                fn.col('SALES_AMT').alias('amount'),
                fn.col('SALES_AMT_TAX_EXCLUDED').alias('amount_without_tax'),
                fn.col('PAYMENTS_DIV').alias('payment_method'),
                fn.col('PAYMENTS_KIND').alias('payment_type'),
                fn.col('SHOP_CD').alias('store_id')
            ).alias('purchase_details'),
            
            # 折扣信息
            fn.struct(
                fn.col('DISCOUNT_PRICE').alias('discount_amount'),
                fn.col('PRICE_DOWN_AMT').alias('price_down_amount'),
                fn.col('DISCOUNT_DIV_1').alias('discount_type1'),
                fn.col('DISCOUNT_DIV_2').alias('discount_type2'),
                fn.col('DISCOUNT_DIV_3').alias('discount_type3')
            ).alias('discount_info')
        ).where(
            fn.col('CUSTOMER_ID').isNotNull()
        ).dropDuplicates(['id'])

    def _create_visit_relationships(self, df):
        """创建访问关系数据"""
        return df.select(
            # 基本信息
            fn.concat('RECEIPT_NO').alias('id'),
            fn.col('CUSTOMER_ID').alias('person_id'),
            fn.col('SHOP_CD').alias('store_id'),
            
            # 时间信息
            fn.concat(
                fn.col('SALES_YMD').cast('string'),
                fn.lit('T'),
                fn.col('SALES_HMS')
            ).alias('datetime'),
            
            # 访问详情
            fn.struct(
                fn.col('COMPANY_CD_UNIQUE').alias('company_id'),
                fn.col('REGISTER_NO').alias('register_no'),
                fn.struct(
                    fn.lit(True).alias('converted'),
                    fn.concat_ws('_', 
                        fn.col('RECEIPT_NO'), 
                        fn.col('SALES_DETAIL_NO')
                    ).alias('purchase_id')
                ).alias('conversion')
            ).alias('visit_details')
        ).where(
            fn.col('CUSTOMER_ID').isNotNull()
        ).dropDuplicates(['id'])

    def save_relationships(self, purchases_df, visits_df, output_dir: str):
        """保存关系数据为JSON格式"""
        try:
            # 保存购买关系
            purchases_df.write.mode('overwrite') \
                .json(f"{output_dir}/purchases.json")
            self.logger.info("Saved purchase relationships")

            # 保存访问关系
            visits_df.write.mode('overwrite') \
                .json(f"{output_dir}/visits.json")
            self.logger.info("Saved visit relationships")

        except Exception as e:
            self.logger.error(f"Error saving relationships: {str(e)}")
            raise

# COMMAND ----------

def main():
        # 初始化提取器
        extractor = RelationshipExtractor(spark)
        
        # 提取关系
        purchases_df, visits_df = extractor.extract_relationships(data)
        
        # 保存关系数据
        extractor.save_relationships(purchases_df, visits_df, cfg.relation_fp)
        
        # 分析关系
        extractor.analyze_relationships(purchases_df, visits_df)

if __name__ == "__main__":
    main()
