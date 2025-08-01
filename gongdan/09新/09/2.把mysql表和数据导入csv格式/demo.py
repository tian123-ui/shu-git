from pyspark.sql import SparkSession
import os

def get_spark_session():
    """初始化 SparkSession 并配置 Hive 连接等"""
    spark = SparkSession.builder \
       .appName("CSVToHDFS") \
       .config("spark.hadoop.hive.metastore.uris", "thrift://cdh01:9083") \
       .config("spark.sql.hive.convertMetastoreOrc", "true") \
       .config("spark.sql.orc.compression.codec", "snappy") \
       .config("hive.exec.dynamic.partition.mode", "nonstrict") \
       .config("spark.sql.hive.ignoreMissingPartitions", "true") \
       .enableHiveSupport() \
       .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    # 创建 gon09 数据库（如果不存在），并切换到该数据库
    spark.sql("CREATE DATABASE IF NOT EXISTS gon09")
    spark.sql("USE gon09")
    return spark

def csv_to_hdfs(spark, csv_file_path, table_name):
    """
    将本地 CSV 文件写入 HDFS 指定路径，并可选择创建 Hive 外部表关联
    :param spark: 已初始化的 SparkSession
    :param csv_file_path: 本地 CSV 文件路径
    :param table_name: HDFS 存储目录名及 Hive 表名（路径为 /warehouse/gon09/table_name ）
    """
    hdfs_base_path = "hdfs://cdh01:8020/warehouse/gon09"  # 根据实际 HDFS 地址调整
    hdfs_table_path = f"{hdfs_base_path}/{table_name}"

    try:
        # 读取 CSV 文件为 DataFrame ，假设 CSV 有表头，自动推断 schema
        df = spark.read.csv(
            csv_file_path,
            header=True,
            inferSchema=True,
            encoding="utf-8"
        )

        # 将 DataFrame 写入 HDFS ，格式为 Parquet（可根据需求改为 CSV 等其他格式）
        # 若存在则覆盖，可根据需求改为 append 等模式
        df.write.mode("overwrite").parquet(hdfs_table_path)
        print(f"[INFO] CSV 文件 {csv_file_path} 已成功写入 HDFS 路径 {hdfs_table_path}")

        # （可选）创建 Hive 外部表，关联 HDFS 上的数据
        create_hive_table_sql = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (
            {', '.join([f"{col} {dtype}" for col, dtype in df.dtypes])}
        )
        STORED AS PARQUET
        LOCATION '{hdfs_table_path}'
        """
        spark.sql(create_hive_table_sql)
        print(f"[INFO] Hive 外部表 {table_name} 已成功创建，关联 HDFS 路径 {hdfs_table_path}")

        # 验证数据，查询前 5 条
        verify_df = spark.sql(f"SELECT * FROM {table_name} LIMIT 5")
        print(f"[INFO] Hive 表 {table_name} 前 5 条数据：")
        verify_df.show()

    except Exception as e:
        print(f"[ERROR] 执行过程中发生错误：{str(e)}")

if __name__ == "__main__":
    spark = get_spark_session()
    # 获取当前目录下的 CSV 文件列表（假设脚本和 CSV 文件在同一目录，可按需修改路径逻辑）
    csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]
    for csv_file in csv_files:
        table_name = csv_file.replace('.csv', '')  # 用 CSV 文件名作为表名（去掉后缀）
        csv_to_hdfs(spark, csv_file, table_name)
    spark.stop()