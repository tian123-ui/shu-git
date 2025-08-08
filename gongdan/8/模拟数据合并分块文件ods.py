import os
import pandas as pd

# 定义各层表对应的分块目录名称（需与实际 mock_data 下目录名匹配）
table_dirs = [
    "ods_customer_service",
    "ods_customer_service_promo_activity",
    "ods_customer_service_promo_activity_item",
    "ods_customer_service_promo_send",
    "ods_customer_service_promo_use",
    "ods_product",
    "ods_sku"
]

def merge_part_files(dir_path, output_path):
    """
    合并分块文件为单个 CSV
    :param dir_path: 分块文件所在目录路径，如 mock_data/ods/ods_customer_service
    :param output_path: 合并后 CSV 输出路径，如 mock_data/ods_customer_service.csv
    """
    try:
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
    mock_data_root = "mock_data/ods"
    for table_dir in table_dirs:
        input_dir = os.path.join(mock_data_root, table_dir)
        # 调整输出路径到 mock_data 目录下
        output_csv = os.path.join("mock_data", f"{table_dir}.csv")
        merge_part_files(input_dir, output_csv)