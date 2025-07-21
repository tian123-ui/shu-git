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
    # 使用partitionBy指定分区列（如果表是分区表）
    jdbcDF.write.mode('append').insertInto(tableName)


def execute_hive_insert(partition_date: str, tableName):
    """从源表查询数据并写入目标Hive分区表"""
    spark = get_spark_session()

    # 构建动态SQL查询（确保列数与目标表一致）
    select_sql1 = f"""
    select * from ads_coupon_stats
    union
    select
        '{partition_date}' as dt,  -- 分区列名必须与目标表一致
        coupon_id,
        coupon_name,
        cast(sum(used_count_1d) as bigint) as used_count,
        cast(count(*) as bigint) as used_user_count
    from dws_tool_user_coupon_coupon_used_1d
    where dt='{partition_date}'
    group by coupon_id, coupon_name;
    """

    print(f"[INFO] 开始执行SQL查询，目标分区：{partition_date}")
    df1 = spark.sql(select_sql1)

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df1.show(5)  # 确认数据列数与目标表一致

    # 写入数据（无需添加多余的ds列）
    select_to_hive(df1, tableName)

    # 验证数据
    print(f"[INFO] 验证分区 {partition_date} 的数据...")
    verify_df = spark.sql(f"SELECT * FROM {tableName} WHERE dt='{partition_date}' LIMIT 5")
    verify_df.show()


if __name__ == "__main__":
    target_date = '2025-06-25'
    execute_hive_insert(target_date, 'ads_coupon_stats')