import pymysql
import csv
from typing import List, Dict

# MySQL 连接配置
CONFIG = {
    'host': 'cdh03',
    'user': 'root',
    'password': 'root',
    'database': 'gon09',
    'port': 3306,
    'charset': 'utf8mb4'
}

def get_table_names() -> List[str]:
    """获取 gon09 数据库中所有表的名称"""
    conn = pymysql.connect(**CONFIG)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SHOW TABLES;")
            results = cursor.fetchall()
            return [table[0] for table in results]
    finally:
        conn.close()

def export_table_to_csv(table_name: str, file_path: str) -> None:
    """
    将指定表的数据导出为 CSV 文件
    :param table_name: 要导出的表名
    :param file_path: 导出的 CSV 文件保存路径
    """
    conn = pymysql.connect(**CONFIG)
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            # 查询表数据
            cursor.execute(f"SELECT * FROM {table_name};")
            rows: List[Dict] = cursor.fetchall()

            if not rows:
                print(f"表 {table_name} 无数据，跳过导出")
                return

            # 获取表头（字段名）
            headers = list(rows[0].keys())

            # 写入 CSV 文件
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                # 写入表头
                writer.writeheader()
                # 写入数据行
                writer.writerows(rows)
            print(f"表 {table_name} 已成功导出至 {file_path}")
    except Exception as e:
        print(f"导出表 {table_name} 时发生错误: {str(e)}")
    finally:
        conn.close()

def main():
    """主流程：导出 gon09 数据库所有表为 CSV 文件"""
    table_names = get_table_names()
    if not table_names:
        print("未找到任何表，导出结束")
        return

    for table in table_names:
        # 定义 CSV 文件保存路径，可根据需求调整（这里保存在当前目录下，以表名命名）
        csv_file_path = f"{table}.csv"
        export_table_to_csv(table, csv_file_path)

if __name__ == "__main__":
    main()