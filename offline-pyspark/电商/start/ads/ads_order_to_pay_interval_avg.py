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

    # 根据表名动态调整SQL查询逻辑
    if tableName == 'ads_order_to_pay_interval_avg':
        # 计算订单支付间隔时间的平均值
        select_sql1 = f"""
        select * from {tableName}
        union
        select
            '{partition_date}' as dt,
            cast(avg(to_unix_timestamp(payment_time)-to_unix_timestamp(order_time)) as bigint) as avg_interval
        from dwd_trade_trade_flow_acc
        where dt in ('9999-12-31','{partition_date}')
          and payment_date_id='{partition_date}';
        """
    elif tableName == 'ads_sku_favor_count_top3_by_tm':
        # 计算每个品牌下收藏次数TOP3的SKU
        select_sql1 = f"""
        select * from {tableName}
        union
        select
            '{partition_date}' as dt,
            tm_id,
            sku_id,
            count(*) as favor_count,
            row_number() over(partition by tm_id order by count(*) desc) as rn
        from dwd_tool_sku_favor
        where dt = '{partition_date}'
        group by sku_id, tm_id
        having rn <= 3;
        """
    else:
        raise ValueError(f"不支持的表名: {tableName}")

    print(f"[INFO] 开始执行SQL查询，目标分区：{partition_date}")
    df1 = spark.sql(select_sql1)

    # 添加分区字段ds
    df_with_partition = df1

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    print(f"[INFO] DataFrame列名: {df_with_partition.columns}")  # 打印列名用于调试
    df_with_partition.show(5)

    # 写入数据
    select_to_hive(df_with_partition, tableName)

    # 验证数据
    print(f"[INFO] 验证分区 {partition_date} 的数据...")
    verify_df = spark.sql(f"SELECT * FROM {tableName} WHERE dt='{partition_date}' LIMIT 5")
    verify_df.show()


if __name__ == "__main__":
    target_date = '2025-06-25'
    execute_hive_insert(target_date, 'ads_order_to_pay_interval_avg')  # 正确