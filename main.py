# Databricks notebook source
# -*- coding: utf-8 -*-
import src
from src.logger import KGLogger
from src.data import read_pos_data, get_target_pos_files
from src.config import Config

import importlib
importlib.reload(src.config)
importlib.reload(src.data)
logger = KGLogger("knowledge_graph")

# COMMAND ----------

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

# COMMAND ----------

from pyspark.sql import functions as fn
data 

# COMMAND ----------

data.show(5)
