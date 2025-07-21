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


def select_to_hive(df, table_name):
    """将DataFrame数据追加写入Hive表"""
    df.write.mode('append').insertInto(table_name)


def execute_hive_insert(partition_date: str, table_name: str):
    """从源表查询数据并写入目标Hive分区表"""
    spark = get_spark_session()

    # 构建动态SQL查询 - 使用dt而不是ds
    select_sql = f"""
    SELECT 
        u.id AS user_id,
        nvl(l.login_date_last, date_format(u.create_time, 'yyyy-MM-dd')) AS login_date_last,
        date_format(u.create_time, 'yyyy-MM-dd') AS login_date_first,
        nvl(l.login_count_td, 1) AS login_count_td
    FROM (
        SELECT id, create_time
        FROM dim_user_zip
        WHERE dt = '99991231'
    ) u
    LEFT JOIN (
        SELECT 
            user_id,
            max(dt) AS login_date_last,  -- 使用dt而不是ds
            count(*) AS login_count_td
        FROM dwd_user_login_inc
        WHERE dt <= '{partition_date}'  -- 添加分区过滤
        GROUP BY user_id
    ) l ON u.id = l.user_id
    """

    print(f"[INFO] 开始执行SQL查询，目标分区：{partition_date}")
    df = spark.sql(select_sql)

    # 添加分区字段dt
    df_with_partition = df.withColumn("dt", lit(partition_date))

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df_with_partition.show(5)

    # 写入数据
    print(f"[INFO] 正在写入数据到{table_name}...")
    select_to_hive(df_with_partition, table_name)

    # 验证数据
    print(f"[INFO] 验证分区 {partition_date} 的数据...")
    spark.sql(f"SELECT * FROM {table_name} WHERE dt='{partition_date}' LIMIT 5").show()


if __name__ == "__main__":
    target_date = '20250701'
    execute_hive_insert(target_date, 'dws_user_user_login_td')