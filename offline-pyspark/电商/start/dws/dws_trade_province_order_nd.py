from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime, timedelta


def get_spark_session():
    """初始化SparkSession并配置Hive连接"""
    spark = SparkSession.builder \
        .appName("ProvinceOrderAggregation") \
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


def select_to_hive(df, table_name):
    """将DataFrame数据写入Hive表"""
    try:
        df.write \
            .mode('append') \
            .format("orc") \
            .insertInto(table_name)
        print(f"[SUCCESS] 数据成功写入表 {table_name}")
    except Exception as e:
        print(f"[ERROR] 写入表 {table_name} 失败: {str(e)}")
        raise


def execute_hive_insert(partition_date: str, table_name: str):
    """从日粒度表聚合数据到多日粒度表"""
    spark = get_spark_session()

    # 计算日期范围
    current_date = datetime.strptime(partition_date, '%Y-%m-%d').date()
    start_date_7d = (current_date - timedelta(days=6)).strftime('%Y-%m-%d')
    start_date_30d = (current_date - timedelta(days=29)).strftime('%Y-%m-%d')

    print(f"[INFO] 处理日期范围: 30天({start_date_30d} 至 {partition_date})")
    print(f"[INFO] 最近7天范围: {start_date_7d} 至 {partition_date}")

    # 构建动态SQL查询 - 使用dt作为分区列
    select_sql = f"""
SELECT
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    -- 最近7天聚合
    SUM(IF(dt >= '{start_date_7d}', order_count_1d, 0)) AS order_count_7d,
    SUM(IF(dt >= '{start_date_7d}', order_original_amount_1d, 0)) AS order_original_amount_7d,
    SUM(IF(dt >= '{start_date_7d}', activity_reduce_amount_1d, 0)) AS activity_reduce_amount_7d,
    SUM(IF(dt >= '{start_date_7d}', coupon_reduce_amount_1d, 0)) AS coupon_reduce_amount_7d,
    SUM(IF(dt >= '{start_date_7d}', order_total_amount_1d, 0)) AS order_total_amount_7d,
    -- 最近30天聚合
    SUM(order_count_1d) AS order_count_30d,
    SUM(order_original_amount_1d) AS order_original_amount_30d,
    SUM(activity_reduce_amount_1d) AS activity_reduce_amount_30d,
    SUM(coupon_reduce_amount_1d) AS coupon_reduce_amount_30d,
    SUM(order_total_amount_1d) AS order_total_amount_30d,
    -- 使用dt作为分区列
    '{partition_date}' AS dt
FROM dws_trade_province_order_1d
WHERE dt BETWEEN '{start_date_30d}' AND '{partition_date}'
GROUP BY province_id, province_name, area_code, iso_code, iso_3166_2
    """

    print("[INFO] 执行聚合查询...")
    agg_df = spark.sql(select_sql)

    # 显示处理结果
    print("[INFO] 聚合结果样例:")
    agg_df.show(5, truncate=False)

    # 写入数据
    print(f"[INFO] 写入数据到 {table_name}...")
    select_to_hive(agg_df, table_name)

    # 验证数据 - 使用dt作为分区列
    print(f"[INFO] 验证分区 {partition_date} 的数据...")
    spark.sql(f"SELECT * FROM {table_name} WHERE dt='{partition_date}' LIMIT 5").show()


if __name__ == "__main__":
    target_date = '2025-06-25'
    execute_hive_insert(target_date, 'dws_trade_province_order_nd')