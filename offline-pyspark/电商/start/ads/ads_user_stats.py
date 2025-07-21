from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


def get_spark_session():
    """初始化SparkSession并配置Hive连接，确保数据库存在"""
    spark = SparkSession.builder \
        .appName("HiveETL") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .config("spark.sql.orc.compression.codec", "snappy") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.sql.hive.ignoreMissingPartitions", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    spark.sql("CREATE DATABASE IF NOT EXISTS gmall")
    spark.sql("USE gmall")

    return spark


def select_to_hive(jdbcDF, tableName):
    """将DataFrame数据追加写入Hive表"""
    jdbcDF.write.mode('append').insertInto(f"{tableName}")


def execute_hive_insert(partition_date: str, tableName):
    """从源表查询数据并写入目标Hive分区表"""
    spark = get_spark_session()

    # 构建动态SQL查询，使用参数化日期
    select_sql1 = f"""
SELECT * FROM {tableName}
UNION
SELECT 
    '{partition_date}' AS dt,
    recent_days,
    SUM(IF(login_date_first >= DATE_ADD('{partition_date}', -recent_days + 1), 1, 0)) AS new_user_count,
    COUNT(*) AS active_user_count
FROM dws_user_user_login_td 
LATERAL VIEW EXPLODE(ARRAY(1, 7, 30)) tmp AS recent_days
WHERE dt = '{partition_date}'
  AND login_date_last >= DATE_ADD('{partition_date}', -recent_days + 1)
GROUP BY recent_days;
    """

    print(f"[INFO] 执行的SQL查询:\n{select_sql1}")
    print(f"[INFO] 开始执行SQL查询，目标分区：{partition_date}")

    try:
        df1 = spark.sql(select_sql1)
    except Exception as e:
        print(f"[ERROR] SQL执行失败: {str(e)}")
        raise

    # 验证DataFrame列结构
    print(f"[INFO] DataFrame列结构: {df1.columns}")

    # 检查是否已经有ds列
    if 'ds' in df1.columns:
        print("[INFO] DataFrame中已存在ds列，无需添加")
        df_with_partition = df1
    else:
        # 添加分区字段ds
        df_with_partition = df1.withColumn("dt", lit(partition_date))

    # 验证最终DataFrame列结构
    print(f"[INFO] 带分区的DataFrame列结构: {df_with_partition.columns}")

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df_with_partition.show(5, truncate=False)

    # 写入数据
    print(f"[INFO] 写入数据到表 {tableName}")
    select_to_hive(df_with_partition, tableName)

    # 验证数据
    print(f"[INFO] 验证分区 {partition_date} 的数据...")
    verify_df = spark.sql(f"SELECT * FROM {tableName} WHERE dt='{partition_date}' LIMIT 5")
    verify_df.show(5, truncate=False)


if __name__ == "__main__":
    target_date = '20250701'
    execute_hive_insert(target_date, 'ads_user_stats')