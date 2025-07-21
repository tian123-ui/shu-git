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


def select_to_hive(df, table_name):
    """将DataFrame数据追加写入Hive表"""
    # 修改写入方式，移除partitionBy()，因为insertInto()会自动识别分区列
    df.write.mode('append').insertInto(table_name)


def execute_hive_insert(partition_date: str, table_name: str):
    """从源表查询数据并写入目标Hive分区表"""
    spark = get_spark_session()

    # 构建动态SQL查询
    select_sql = f"""
    SELECT
        od.user_id,
        sku.id as sku_id,
        sku.sku_name,
        sku.category1_id,
        sku.category1_name,
        sku.category2_id,
        sku.category2_name,
        sku.category3_id,
        sku.category3_name,
        sku.tm_id,
        sku.tm_name,
        od.order_count_1d,
        od.order_num_1d,
        od.order_original_amount_1d,
        od.activity_reduce_amount_1d,
        od.coupon_reduce_amount_1d,
        od.order_total_amount_1d,
        '{partition_date}' AS dt  -- 统一使用dt作为分区列
    FROM
        (
            SELECT
                dt,
                user_id,
                sku_id,
                COUNT(*) order_count_1d,
                SUM(sku_num) order_num_1d,
                SUM(split_original_amount) order_original_amount_1d,
                SUM(NVL(split_activity_amount, 0.0)) activity_reduce_amount_1d,
                SUM(NVL(split_coupon_amount, 0.0)) coupon_reduce_amount_1d,
                SUM(split_total_amount) order_total_amount_1d
            FROM dwd_trade_order_detail_inc
            WHERE dt = '{partition_date}'
            GROUP BY dt, user_id, sku_id
        ) od
    LEFT JOIN
        (
            SELECT
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
            FROM dim_sku_full
            WHERE dt = '{partition_date}'
        ) sku
    ON od.sku_id = sku.id
    """

    print(f"[INFO] 开始执行SQL查询，目标分区：{partition_date}")
    df = spark.sql(select_sql)

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df.show(5)
    print(f"[INFO] DataFrame列数: {len(df.columns)}")

    # 写入数据
    print(f"[INFO] 正在写入数据到表{table_name}...")
    select_to_hive(df, table_name)

    # 验证数据
    print(f"[INFO] 验证分区 {partition_date} 的数据...")
    spark.sql(f"SELECT * FROM {table_name} WHERE dt='{partition_date}' LIMIT 5").show()


if __name__ == "__main__":
    target_date = '20250701'
    execute_hive_insert(target_date, 'dws_trade_user_sku_order_1d')