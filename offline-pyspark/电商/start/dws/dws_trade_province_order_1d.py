from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def get_spark_session():
    """初始化SparkSession并配置Hive连接，确保数据库存在"""
    spark = SparkSession.builder \
        .appName("HiveETL") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
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


def execute_hive_insert(partition_date: str, tableName: str):
    """从源表查询数据并写入目标Hive分区表"""
    spark = get_spark_session()

    # 构建动态SQL查询 - 修正了列名和分区字段
    select_sql1 = f"""
select
    o.province_id,
    p.province_name,
    p.area_code,
    p.iso_code,
    p.iso_3166_2,
    o.order_count_1d,
    o.order_original_amount_1d,
    o.activity_reduce_amount_1d,
    o.coupon_reduce_amount_1d,
    o.order_total_amount_1d,
    o.dt as ds  -- 使用源表的dt列并重命名为ds
from
    (
        select
            province_id,
            dt,
            count(distinct(order_id)) order_count_1d,
            sum(split_original_amount) order_original_amount_1d,
            sum(nvl(split_activity_amount,0)) activity_reduce_amount_1d,
            sum(nvl(split_coupon_amount,0)) coupon_reduce_amount_1d,
            sum(split_total_amount) order_total_amount_1d
        from dwd_trade_order_detail_inc
        where dt='{partition_date}'  -- 添加分区过滤
        group by province_id, dt
    ) o
    left join
    (
        select
            id,
            province_name,
            area_code,
            iso_code,
            iso_3166_2
        from dim_province_full
        where dt='{partition_date}'  -- 使用dt作为分区列
    ) p
    on o.province_id = p.id
    """

    print(f"[INFO] 开始执行SQL查询，目标分区：{partition_date}")
    df1 = spark.sql(select_sql1)

    # 显示查询结果结构
    print("[INFO] 查询结果结构:")
    df1.printSchema()
    df1.show(5)

    # 写入数据
    print(f"[INFO] 正在写入数据到表 {tableName}...")
    select_to_hive(df1, tableName)

    # 验证数据 - 使用ds作为分区列
    print(f"[INFO] 验证分区 {partition_date} 的数据...")
    verify_df = spark.sql(f"SELECT * FROM {tableName} WHERE dt='{partition_date}' LIMIT 5")
    verify_df.show()


if __name__ == "__main__":
    target_date = '20250701'
    execute_hive_insert(target_date, 'dws_trade_province_order_1d')