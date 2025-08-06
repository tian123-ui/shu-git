import pandas as pd
from sqlalchemy import create_engine
import os

# MySQL 连接信息
host = "cdh03"
user = "root"
password = "root"
database = "gon03"
engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

# 定义各层表的 CSV 文件路径和表名映射，根据实际文件存放路径调整
table_mapping = {
    # ODS 层（5张）
    "ods_sales_original": "mock_data/ods_sales_original.csv",
    "ods_traffic_original": "mock_data/ods_traffic_original.csv",
    "ods_user_behavior_original": "mock_data/ods_user_behavior_original.csv",
    "ods_content_original": "mock_data/ods_content_original.csv",
    "ods_comment_original": "mock_data/ods_comment_original.csv",
    # DIM 层（4张）
    "dim_platform_channel": "mock_data/dim_platform_channel.csv",
    "dim_product_sku": "mock_data/dim_product_sku.csv",
    "dim_user": "mock_data/dim_user.csv",
    "dim_price_band": "mock_data/dim_price_band.csv",
    # DWD 层（5张）
    "dwd_sku_sales_detail": "mock_data/dwd_sku_sales_detail.csv",
    "dwd_traffic_channel_detail": "mock_data/dwd_traffic_channel_detail.csv",
    "dwd_title_keyword_detail": "mock_data/dwd_title_keyword_detail.csv",
    "dwd_content_effect_detail": "mock_data/dwd_content_effect_detail.csv",
    "dwd_rfm_behavior_detail": "mock_data/dwd_rfm_behavior_detail.csv",
    # DWS 层（4张）
    "dws_sales_summary": "mock_data/dws_sales_summary.csv",
    "dws_price_analysis_summary": "mock_data/dws_price_analysis_summary.csv",
    "dws_rfm_summary": "mock_data/dws_rfm_summary.csv",
    "dws_content_summary": "mock_data/dws_content_summary.csv",
    # ADS 层（5张）
    "ads_product_core_overview": "mock_data/ads_product_core_overview.csv",
    "ads_sku_hot_sale": "mock_data/ads_sku_hot_sale.csv",
    "ads_price_strategy": "mock_data/ads_price_strategy.csv",
    "ads_title_optimization": "mock_data/ads_title_optimization.csv",
    "ads_rfm_strategy": "mock_data/ads_rfm_strategy.csv"
}


def csv_to_mysql():
    success_tables = []
    failed_tables = []
    for table_name, csv_path in table_mapping.items():
        try:
            if not os.path.exists(csv_path):
                raise FileNotFoundError(f"文件 {csv_path} 不存在")

            # 读取 CSV 文件，header=0 表示第一行是表头，可按需添加 encoding 参数（如 encoding='utf-8' ）
            df = pd.read_csv(csv_path, header=0)
            # 处理空值，可根据实际需求调整填充值（如数值列填充0等），这里简单填充空字符串
            df = df.fillna("")

            # 将数据写入 MySQL，if_exists='replace' 会覆盖已有表，若需追加用 'append'
            df.to_sql(table_name, con=engine, if_exists='replace', index=False)
            print(f"成功将 {csv_path} 导入 {table_name} 表")
            success_tables.append(table_name)
        except Exception as e:
            print(f"导入 {table_name} 表失败：{str(e)}")
            failed_tables.append(table_name)

    # 打印导入结果统计
    print(f"\n共处理 {len(table_mapping)} 张表")
    print(f"✅ 成功导入 {len(success_tables)} 张表：{success_tables}")
    print(f"❌ 失败导入 {len(failed_tables)} 张表：{failed_tables}")


if __name__ == "__main__":
    csv_to_mysql()