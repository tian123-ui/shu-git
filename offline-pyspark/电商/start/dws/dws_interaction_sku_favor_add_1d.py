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

    # 构建动态SQL查询 - 修正了列名引用问题
    select_sql1 = f"""
select
    favor.sku_id,
    sku.sku_name,
    sku.category1_id,
    sku.category1_name,
    sku.category2_id,
    sku.category2_name,
    sku.category3_id,
    sku.category3_name,
    sku.tm_id,
    sku.tm_name,
    favor.favor_add_count,
    favor.dt as ds  -- 使用dt作为分区字段并重命名为ds
from
    (
        select
            dt,  -- 使用实际的dt列而不是ds
            sku_id,
            count(*) favor_add_count
        from dwd_interaction_favor_add_inc
        where dt='{partition_date}'
        group by dt, sku_id
    )favor
    left join
    (
        select
            id,
            sku_name,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            tm_id,
            tm_name
        from dim_sku_full
        where dt='{partition_date}'
    )sku
    on favor.sku_id=sku.id
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

    # 验证数据
    print(f"[INFO] 验证分区 {partition_date} 的数据...")
    verify_df = spark.sql(f"SELECT * FROM {tableName} WHERE dt='{partition_date}' LIMIT 5")
    verify_df.show()


if __name__ == "__main__":
    target_date = '20250701'
    execute_hive_insert(target_date, 'dws_interaction_sku_favor_add_1d')