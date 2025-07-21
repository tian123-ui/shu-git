from pyspark.sql import SparkSession
import traceback


def get_spark_session():
    """初始化SparkSession并配置Hive连接"""
    spark = SparkSession.builder \
        .appName("PromotionPosETL") \
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


def execute_promotion_pos_etl(partition_date: str):
    """执行促销位点维度ETL"""
    try:
        spark = get_spark_session()

        print(f"开始执行促销位点维度ETL，分区: {partition_date}")

        # 创建外部表（指定正确分区字段 dt）
        print("创建外部表 dim_promotion_pos_full（如果不存在）")
        spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS dim_promotion_pos_full
        (
            `id` STRING COMMENT '促销位点ID',
            `pos_location` STRING COMMENT '位点位置',
            `pos_type` STRING COMMENT '位点类型',
            `promotion_type` STRING COMMENT '促销类型',
            `create_time` STRING COMMENT '创建时间',
            `operate_time` STRING COMMENT '操作时间'
        )
        COMMENT '促销位点维度表'
        PARTITIONED BY (`dt` STRING)  -- 分区字段改为 dt
        STORED AS ORC
        LOCATION '/bigdata_warehouse/gmall/dim/dim_promotion_pos_full/'
        TBLPROPERTIES ('orc.compress' = 'snappy')
        """)

        # 执行数据加载（使用 dt 作为分区字段）
        print(f"正在加载数据到dim_promotion_pos_full，分区dt={partition_date}")
        insert_sql = f"""
        INSERT OVERWRITE TABLE dim_promotion_pos_full PARTITION(dt='{partition_date}')
        SELECT 
            `id`,
            `pos_location`,
            `pos_type`,
            `promotion_type`,
            `create_time`,
            `operate_time`
        FROM ods_promotion_pos
        WHERE dt='{partition_date}'
        """

        result = spark.sql(insert_sql)
        print(f"数据加载完成，影响行数: {result.count()}")

        # 验证数据（用 dt 过滤）
        print(f"验证分区 dt={partition_date} 的数据:")
        verify_df = spark.sql(f"SELECT * FROM dim_promotion_pos_full WHERE dt='{partition_date}' LIMIT 5")
        verify_df.show()
        print(f"验证完成，分区 dt={partition_date} 包含 {verify_df.count()} 条样本数据")

        print(f"促销位点维度ETL执行成功，分区: {partition_date}")

    except Exception as e:
        print(f"ETL执行失败: {str(e)}")
        traceback.print_exc()
        raise


if __name__ == "__main__":
    execute_promotion_pos_etl('20250701')