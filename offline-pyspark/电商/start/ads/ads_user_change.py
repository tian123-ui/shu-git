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
    jdbcDF.write.mode('append').insertInto(f"{tableName}")


def execute_hive_insert(partition_date: str, tableName):
    """从源表查询数据并写入目标Hive分区表"""
    spark = get_spark_session()

    # 构建动态SQL查询
    select_sql1 = f"""
 SELECT * FROM {tableName}
    UNION
    SELECT
        churn.dt,
        user_churn_count,
        user_back_count
    FROM
        (
            SELECT
                '{partition_date}' dt,
                COUNT(*) user_churn_count
            FROM dws_user_user_login_td
            WHERE dt='{partition_date}' 
              AND login_date_last=date_add('{partition_date}',-7)
        ) churn
        JOIN
        (
            SELECT
                '{partition_date}' AS dt,
                COUNT(*) user_back_count
            FROM
                (
                    SELECT
                        user_id,
                        login_date_last
                    FROM dws_user_user_login_td
                    WHERE dt='{partition_date}'
                      AND login_date_last = '{partition_date}'
                ) t1
                JOIN
                (
                    SELECT
                        user_id,
                        login_date_last login_date_previous
                    FROM dws_user_user_login_td
                    WHERE dt=date_add('{partition_date}',-1)
                ) t2
                ON t1.user_id=t2.user_id
            WHERE datediff(login_date_last,login_date_previous)>=8
        ) back
        ON churn.dt=back.dt;
    """

    print(f"[INFO] 开始执行SQL查询，目标分区：{partition_date}")
    df1 = spark.sql(select_sql1)

    # 添加分区字段ds
    df_with_partition = df1.withColumn("dt", lit(partition_date))

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df_with_partition.show(5)

    # 写入数据
    select_to_hive(df_with_partition, tableName)

    # 验证数据
    print(f"[INFO] 验证分区 {partition_date} 的数据...")
    verify_df = spark.sql(f"SELECT * FROM {tableName} WHERE dt='{partition_date}' LIMIT 5")
    verify_df.show()


if __name__ == "__main__":
    target_date = '20250701'
    execute_hive_insert(target_date, 'ads_user_change')