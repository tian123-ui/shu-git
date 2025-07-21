from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def get_spark_session():
    """初始化SparkSession并配置Hive连接，确保数据库存在"""
    spark = SparkSession.builder \
        .appName("HiveETL") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .config("spark.sql.orc.compression.codec", "snappy")   \
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

    # 构建动态SQL查询，修正了ds为dt
    select_sql1 = f"""
SELECT * FROM ads_traffic_stats_by_channel
UNION
SELECT
    '{partition_date}' AS dt,  -- 明确指定列别名
    recent_days,
    channel,
    CAST(COUNT(DISTINCT(mid_id)) AS BIGINT) AS uv_count,
    CAST(AVG(during_time_1d)/1000 AS BIGINT) AS avg_duration_sec,
    CAST(AVG(page_count_1d) AS BIGINT) AS avg_page_count,
    CAST(COUNT(*) AS BIGINT) AS sv_count,
    CAST(SUM(IF(page_count_1d=1,1,0))/COUNT(*) AS DECIMAL(16,2)) AS bounce_rate
FROM dws_traffic_session_page_view_1d 
LATERAL VIEW EXPLODE(ARRAY(1,7,30)) tmp AS recent_days
WHERE dt >= DATE_ADD('{partition_date}', -recent_days + 1)  -- 使用dt列进行过滤
GROUP BY recent_days, channel;
    """

    print(f"[INFO] 开始执行SQL查询，目标分区：{partition_date}")
    df1 = spark.sql(select_sql1)

    # 注意：原代码在这里又添加了一个ds列，可能会导致重复。如果上面SQL中已经设置了ds，这里可以删除
    # df_with_partition = df1.withColumn("ds", lit(partition_date))

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df1.show(5)

    # 写入数据
    select_to_hive(df1, tableName)  # 直接使用df1，不需要额外添加ds列

    # 验证数据
    print(f"[INFO] 验证分区 {partition_date} 的数据...")
    verify_df = spark.sql(f"SELECT * FROM {tableName} WHERE ds='{partition_date}' LIMIT 5")
    verify_df.show()


if __name__ == "__main__":
    target_date = '20250701'
    execute_hive_insert(target_date, 'ads_traffic_stats_by_channel')