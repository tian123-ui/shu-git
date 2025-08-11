import os
import pandas as pd
from sqlalchemy import create_engine, text
import chardet

# 数据库连接信息
DB_HOST = "cdh03"
DB_USER = "root"
DB_PASSWORD = "root"
DB_NAME = "gon08"
# 创建数据库连接引擎
engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")

# 指定要处理的 CSV 文件列表，按照你提供的文件清单整理
csv_file_list = [
    "ads_customer_service_performance.csv",
    "ads_customer_service_promo_activity_effect.csv",
    "ads_customer_service_promo_overview.csv",
    "ads_customer_service_promo_send_detail.csv",
    "dim_activity_type.csv",
    "dim_customer_service.csv",
    "dim_product.csv",
    "dim_shop.csv",
    "dim_sku.csv",
    "dwd_customer_service_promo_activity.csv",
    "dwd_customer_service_promo_activity_item.csv",
    "dwd_customer_service_promo_send.csv",
    "dwd_customer_service_promo_use.csv",
    "dws_customer_service_promo_activity.csv",
    "dws_customer_service_promo_cs.csv",
    "dws_customer_service_promo_daily.csv",
    "dws_customer_service_promo_product.csv",
    "ods_customer_service.csv",
    "ods_customer_service_promo_activity.csv",
    "ods_customer_service_promo_activity_item.csv",
    "ods_customer_service_promo_send.csv",
    "ods_customer_service_promo_use.csv"
]

# 检测文件编码的函数
def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read(10000))  # 读取部分内容检测编码
    return result['encoding'] or 'utf-8'  # 编码不存在时默认用 utf-8

# 将 CSV 文件转换为 MySQL 表并导入数据，同时添加注释的函数
def csv_to_mysql(csv_file_name):
    """
    处理单个 CSV 文件，创建对应的 MySQL 表，添加字段注释并导入数据
    :param csv_file_name: CSV 文件名（需确保在 mock_data 目录下）
    """
    file_path = os.path.join("mock_data", csv_file_name)
    table_name = os.path.splitext(csv_file_name)[0]  # 从文件名获取表名，去掉 .csv 后缀

    # 检测文件编码
    encoding = detect_encoding(file_path)
    # 读取 CSV 文件内容，设置 encoding 参数
    df = pd.read_csv(file_path, encoding=encoding)

    # 定义 Pandas 数据类型到 MySQL 数据类型的映射
    dtype_mapping = {
        'int64': 'INT',
        'float64': 'DECIMAL(10,2)',
        'object': 'VARCHAR(255)',
        'datetime64[ns]': 'DATETIME'
    }

    # 用于存储每个字段对应的数据类型，后续添加注释时使用
    column_type_dict = {}

    # 构建创建表的 SQL 语句
    create_table_sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n"
    columns = df.columns
    for idx, col in enumerate(columns):
        # 获取 Pandas 数据类型并映射为 MySQL 数据类型
        pandas_dtype = str(df[col].dtype)
        mysql_dtype = dtype_mapping.get(pandas_dtype, 'VARCHAR(255)')
        column_type_dict[col] = mysql_dtype

        # 拼接字段定义语句
        field_def = f"`{col}` {mysql_dtype}"
        # 简单判断主键，假设字段名包含 'id' 且是第一个字段时设为主键
        if 'id' in col.lower() and idx == 0:
            field_def += " PRIMARY KEY"
        else:
            # 非主键字段允许为空
            field_def += " NULL"

        # 处理字段定义语句末尾的逗号，最后一个字段不加逗号
        if idx < len(columns) - 1:
            field_def += ","
        create_table_sql += f"  {field_def}\n"
    # 添加表注释，将下划线替换为空格并标题化
    table_comment = table_name.replace('_', ' ').title()
    create_table_sql += f") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='{table_comment}';"

    # 执行 SQL 操作，创建表、添加注释、导入数据
    with engine.connect() as conn:
        # 执行创建表语句
        conn.execute(text(create_table_sql))
        conn.commit()

        # 为每个字段添加注释
        for col, dtype in column_type_dict.items():
            # 生成字段注释，将下划线替换为空格并标题化
            col_comment = col.replace('_', ' ').title()
            # 完整的修改字段并添加注释的 SQL 语句
            alter_sql = f"ALTER TABLE `{table_name}` MODIFY COLUMN `{col}` {dtype} COMMENT '{col_comment}'"
            conn.execute(text(alter_sql))
        conn.commit()

        # 处理空值，将 DataFrame 中的空值替换为 None，避免导入数据库时报错
        df = df.where(pd.notnull(df), None)
        # 将数据导入到创建好的表中，if_exists='append' 表示追加数据
        df.to_sql(table_name, con=engine, if_exists='append', index=False)

if __name__ == "__main__":
    # 遍历要处理的 CSV 文件列表，逐个导入
    for csv_file in csv_file_list:
        try:
            print(f"开始处理文件：{csv_file}")
            csv_to_mysql(csv_file)
            print(f"文件 {csv_file} 处理完成，对应的表已创建并导入数据\n")
        except Exception as e:
            print(f"处理文件 {csv_file} 时发生错误：{str(e)}")
            print("跳过当前文件，继续处理下一个...\n")