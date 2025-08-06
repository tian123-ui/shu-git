
#TODO:合并分块文件：如果这些分块文件是你需要的数据源，需要先合并成一个完整的 CSV 。可以用简单的 Python 代码合并

########################################################################################################

import os
import pandas as pd

# 定义各层表对应的分块目录名称（需与实际 mock_data 下目录名匹配）
table_dirs = [
    # ODS 层
    "ods_sales_original",
    "ods_traffic_original",
    "ods_user_behavior_original",
    "ods_content_original",
    "ods_comment_original",
    # DIM 层
    "dim_platform_channel",
    "dim_product_sku",
    "dim_user",
    "dim_price_band",
    # DWD 层
    "dwd_sku_sales_detail",
    "dwd_traffic_channel_detail",
    "dwd_title_keyword_detail",
    "dwd_content_effect_detail",
    "dwd_rfm_behavior_detail",
    # DWS 层
    "dws_sales_summary",
    "dws_price_analysis_summary",
    "dws_rfm_summary",
    "dws_content_summary",
    # ADS 层（假设也有分块情况，按实际补充）
    "ads_price_strategy",
    "ads_product_core_overview",
    "ads_rfm_strategy",
    "ads_sku_hot_sale",
    "ads_title_optimization"
]

def merge_part_files(dir_path, output_path):
    """
    合并分块文件为单个 CSV
    :param dir_path: 分块文件所在目录路径
    :param output_path: 合并后 CSV 输出路径
    """
    try:
        # 存储所有分块文件的数据
        data_frames = []
        header_written = False
        for root, dirs, files in os.walk(dir_path):
            for file in files:
                if file.startswith('part-'):  # 筛选分块文件
                    file_full_path = os.path.join(root, file)
                    df = pd.read_csv(file_full_path)
                    if not header_written:
                        # 写入表头
                        df.to_csv(output_path, index=False, mode='w')
                        header_written = True
                    else:
                        # 仅写入数据行，跳过表头
                        df.to_csv(output_path, index=False, header=False, mode='a')
        print(f"成功合并 {dir_path} 到 {output_path}")
    except Exception as e:
        print(f"合并 {dir_path} 失败：{str(e)}")

if __name__ == "__main__":
    # mock_data 根目录，需根据实际项目结构确认，若代码和 mock_data 同级，可这样写
    mock_data_root = "mock_data"
    for table_dir in table_dirs:
        input_dir = os.path.join(mock_data_root, table_dir)
        output_csv = os.path.join(mock_data_root, f"{table_dir}.csv")
        merge_part_files(input_dir, output_csv)