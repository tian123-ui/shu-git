from pyspark.sql import SparkSession


def get_spark_session():
    """初始化SparkSession并配置Hive连接"""
    spark = SparkSession.builder \
        .appName("PromotionReferETL") \
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


def execute_promotion_refer_etl(partition_date: str):
    """执行促销参考维度ETL"""
    spark = get_spark_session()

    # 查看源表字段（可选，用于调试）
    print("源表 ods_promotion_refer 字段：")
    spark.sql("DESCRIBE ods_promotion_refer").show()

    # 创建外部表（字段与源表匹配）
    create_sql = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS dim_promotion_refer_full (
        `id` STRING COMMENT '推荐ID',
        `refer_name` STRING COMMENT '推荐名称',
        `create_time` STRING COMMENT '创建时间',
        `operate_time` STRING COMMENT '操作时间'
    )
    COMMENT '促销推荐维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/bigdata_warehouse/gmall/dim/dim_promotion_refer_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy')
    """
    spark.sql(create_sql)

    # 执行数据加载（仅使用源表存在的字段）
    insert_sql = f"""
    INSERT OVERWRITE TABLE dim_promotion_refer_full PARTITION(dt='{partition_date}')
    SELECT 
        `id`,
        `refer_name`,
        `create_time`,
        `operate_time`
    FROM ods_promotion_refer
    WHERE dt='{partition_date}'
    """

    print(f"正在加载数据到dim_promotion_refer_full，分区dt={partition_date}")
    spark.sql(insert_sql)

    # 验证数据
    print("数据验证：")
    spark.sql(f"SELECT * FROM dim_promotion_refer_full WHERE dt='{partition_date}' LIMIT 5").show()


if __name__ == "__main__":
    execute_promotion_refer_etl('20250701')