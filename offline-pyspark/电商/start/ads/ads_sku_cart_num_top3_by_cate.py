from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def get_spark_session():
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
    jdbcDF.write.mode('append').insertInto(tableName)

def execute_hive_insert(partition_date: str, tableName):
    spark = get_spark_session()

    select_sql1 = f"""
    select
        '{partition_date}' as dt,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        sku_id,
        sku_name,
        cart_num,
        rk
    from
        (
            select
                sku_id,
                sku_name,
                category1_id,
                category1_name,
                category2_id,
                category2_name,
                category3_id,
                category3_name,
                cart_num,
                rank() over (partition by category1_id,category2_id,category3_id order by cart_num desc) rk
            from
                (
                    select
                        sku_id,
                        sum(sku_num) cart_num
                    from dwd_trade_cart_full
                    where dt='{partition_date}'
                    group by sku_id
                )cart
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
                        category3_name
                    from dim_sku_full
                    where dt='{partition_date}'
                )sku
                on cart.sku_id=sku.id
        )t1
    where rk<=3
    """

    print(f"[INFO] 开始执行SQL查询，目标分区：{partition_date}")
    df1 = spark.sql(select_sql1)

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df1.show(5)

    print(f"[INFO] 写入数据到表{tableName}...")
    select_to_hive(df1, tableName)

    print(f"[INFO] 验证分区 {partition_date} 的数据...")
    verify_df = spark.sql(f"SELECT * FROM {tableName} WHERE dt='{partition_date}' LIMIT 5")
    verify_df.show()

if __name__ == "__main__":
    target_date = '20250701'
    execute_hive_insert(target_date, 'ads_sku_cart_num_top3_by_cate')