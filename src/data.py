from typing import List, Dict, Any
import pyspark.sql.functions as fn
import pandas as pd

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