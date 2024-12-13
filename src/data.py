from typing import List, Dict, Any
import pyspark.sql.functions as fn
import pandas as pd

from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from typing import Dict, List
import logging
import json


def get_target_pos_files(
        campaign_start_date:str,
        tbl_sales_details:str,
        periods:int=1
    )->List[str]:

    """
    この関数は、キャンペーン開始月から前6ヶ月の期間に基づいた売上詳細ファイルのパスリストを生成します。

    引数:
        - campaign_start_date: キャンペーン開始日を "YYYYMM" 形式で表した文字列。
        - tbl_sales_details: 売上詳細データのファイルパスのベースディレクトリまたはテーブル名。
        - periods: 期間（月数）を指定する整数。ただし、現時点では使用されていない。

    戻り値:
        - 指定された6ヶ月分の売上詳細ファイルのパスを含むリストを返します。
    """

    # 定义 campaign_month
    campaign_start_date = pd.to_datetime(
        campaign_start_date, 
        format="%Y%m"
    )

    # 计算不包含 campaign_month 的前六个月的月份
    previous_six_months = pd.date_range(
        end=campaign_start_date - pd.DateOffset(months=0), 
        periods=periods, 
        freq='M'
    )

    # 将结果格式化为"YYYYMM"
    previous_six_months_str = (
        previous_six_months.strftime("%Y%m").tolist()
    )
    
    return [
        f"{tbl_sales_details}/{month}*/*" for month in previous_six_months_str
    ]

    
def read_pos_data(spark, 
                  file_path:List[str], 
                  cols:List[str]|None,
                  comp_cd:str,
    ):

    pos_data = (
        spark.read.parquet(*file_path)
    )

    # pos_data = (
    #     pos_data.select(*cols)
    # )

    pos_data = (
        pos_data.filter(
            (fn.col("SALES_SIGN")=="+")&\
            (fn.col("COUNTRY_CD")=="10")&\
            (fn.col("COMPANY_CD_UNIQUE")==comp_cd)
        )
    )
        
    return pos_data



class DataTransformer:
    """数据转换处理类"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = self._setup_logger()
        
    @staticmethod
    def _setup_logger():
        """设置日志"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)

    def read_pyspark_dataframe(self, df) -> None:
        """读取parquet文件"""
        try:
            self.df = df
            self.logger.info(f"Successfully read df")
        except Exception as e:
            self.logger.error(f"Error reading parquet file: {str(e)}")
            raise

    def transform_persons(self) -> None:
        """转换人员数据"""
        try:
            # 构建生日字段
            birthday_concat = fn.concat_ws('-', 
                fn.col('BIRTHDAY_Y'),
                fn.lpad('BIRTHDAY_M', 2, '0'),
                fn.lpad('BIRTHDAY_D', 2, '0')
            ).alias('birth_date')

            person_columns = [
                fn.col('CUSTOMER_ID').alias('id'),
                fn.col('SEX').alias('gender'),
                birthday_concat,
                # 构建location
                fn.struct(
                    fn.col('ZIP_CODE').alias('zip_code'),
                    fn.col('PREFECTURE').alias('prefecture'),
                    fn.struct(
                        fn.col('LATITUDE').alias('latitude'),
                        fn.col('LONGITUDE').alias('longitude')
                    ).alias('coordinates')
                ).alias('location'),
                # 构建contact_preferences
                fn.struct(
                    (fn.col('DM_STOP_TYPE') == 'Y').alias('dm_stop'),
                    (fn.col('EMAIL_STOP_TYPE') == 'Y').alias('email_stop'),
                    (fn.col('TEL_STOP_TYPE') == 'Y').alias('tel_stop'),
                    (fn.col('MAIL_PC_FLG') == 'Y').alias('mail_pc'),
                    (fn.col('MAIL_MOBILE_FLG') == 'Y').alias('mail_mobile')
                ).alias('contact_preferences'),
                # 构建membership
                fn.struct(
                    fn.col('MEMBER_ID').alias('member_id'),
                    fn.col('CARD_ID').alias('card_id'),
                    fn.col('CARD_STS').alias('status'),
                    fn.to_timestamp('START_DATE').alias('start_date'),
                    fn.to_timestamp('END_DATE').alias('end_date'),
                    fn.to_timestamp('EXPIRATION_DATE').alias('expiration_date'),
                    fn.to_timestamp('REGIST_YMD').alias('registration_date')
                ).alias('membership')
            ]
            
            # 缓存频繁使用的数据
            self.persons_df = self.df.select(person_columns) \
                                   .dropDuplicates(['id']) \
                                   .cache()
            
            self.logger.info("Successfully transformed persons data")
        except Exception as e:
            self.logger.error(f"Error transforming persons data: {str(e)}")
            raise

    def transform_products(self) -> None:
        """转换商品数据"""
        try:
            product_columns = [
                fn.col('ITEM_CD_UNIQUE').alias('id'),
                fn.col('PD_NM').alias('name'),
                fn.col('PD_NM_KANA').alias('name_kana'),
                # 构建categories
                fn.struct(
                    fn.col('ITEM_CATEGORY_LV1_CD').alias('major_category'),
                    fn.col('ITEM_CATEGORY_LV2_CD').alias('medium_category'),
                    fn.col('ITEM_CATEGORY_LV3_CD').alias('minor_category'),
                    fn.struct(
                        fn.col('GRP_CD').alias('code'),
                        fn.col('GRP_KJ').alias('name'),
                        fn.col('GRP_KN').alias('name_kana')
                    ).alias('group'),
                    fn.struct(
                        fn.col('DEPT_CD').alias('code'),
                        fn.col('DEPT_KJ').alias('name'),
                        fn.col('DEPT_KN').alias('name_kana')
                    ).alias('department')
                ).alias('categories'),
                # 构建attributes
                fn.struct(
                    fn.struct(
                        fn.col('CLR_CD').alias('code'),
                        fn.col('CLR_NM').alias('name')
                    ).alias('color'),
                    fn.struct(
                        fn.col('SZ_CD').alias('code'),
                        fn.col('SZ_NM').alias('name')
                    ).alias('size'),
                    fn.col('MFC_CD').alias('maker_code'),
                    fn.col('SSN_CD').alias('season_code'),
                    fn.col('VARIATION_PRODUCT_NO').alias('variation_no')
                ).alias('attributes'),
                # 构建pricing
                fn.struct(
                    fn.col('MASTER_PRICE').cast('decimal(14,2)').alias('master_price'),
                    fn.col('SCHEDULE_PRICE').cast('decimal(14,2)').alias('schedule_price'),
                    fn.col('HYOUJUN_BAITANKA').cast('decimal(14,2)').alias('standard_price'),
                    fn.col('HYOUJUN_GENTANKA').cast('decimal(14,2)').alias('standard_cost'),
                    fn.col('ZEIRITSU').cast('integer').alias('tax_rate')
                ).alias('pricing')
            ]
            
            # 缓存频繁使用的数据
            self.products_df = self.df.select(product_columns) \
                                    .dropDuplicates(['id']) \
                                    .cache()
            
            self.logger.info("Successfully transformed products data")
        except Exception as e:
            self.logger.error(f"Error transforming products data: {str(e)}")
            raise

    def transform_stores(self) -> None:
        """转换店铺数据"""
        try:
            # 窗口函数用于聚合register numbers
            window_spec = Window.partitionBy('SHOP_CD', 'COMPANY_CD_UNIQUE')
            
            store_columns = [
                fn.col('SHOP_CD').alias('id'),
                fn.col('COMPANY_CD_UNIQUE').alias('company_id'),
                fn.collect_set('REGISTER_NO').over(window_spec).alias('register_numbers')
            ]
            
            # 缓存频繁使用的数据
            self.stores_df = self.df.select(store_columns) \
                                  .dropDuplicates(['id', 'company_id']) \
                                  .cache()
            
            self.logger.info("Successfully transformed stores data")
        except Exception as e:
            self.logger.error(f"Error transforming stores data: {str(e)}")
            raise

    def save_to_json(self, output_dir: str) -> None:
        """保存转换后的数据为JSON文件"""
        try:
            # 转换为JSON格式并保存
            datasets = {
                'persons': self.persons_df,
                'products': self.products_df,
                'stores': self.stores_df
            }
            
            for name, df in datasets.items():
                output_path = f"{output_dir}/{name}.json"
                df.write.mode('overwrite') \
                    .json(output_path)
                self.logger.info(f"Successfully saved {name} data to {output_path}")
                
        except Exception as e:
            self.logger.error(f"Error saving data to JSON: {str(e)}")
            raise
        finally:
            # 清理缓存
            for df in [self.persons_df, self.products_df, self.stores_df]:
                if df is not None:
                    df.unpersist()

    def transform_all(self) -> None:
        """执行所有转换"""
        self.transform_persons()
        self.transform_products()
        self.transform_stores()





