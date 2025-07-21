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
    """将DataFrame数据追加写入Hive表（无需重复指定分区）"""
    jdbcDF.write.mode('append').insertInto(f"{tableName}")  # 移除 partitionBy("ds")

def execute_hive_insert(partition_date: str, tableName):
    """从源表查询数据并写入目标Hive分区表"""
    spark = get_spark_session()

    # 构建动态SQL查询（直接在SQL中添加ds字段）
    select_sql1 = f"""
    SELECT
        rule.id,
        info.id,
        activity_name,
        rule.activity_type,
        dic.dic_name,
        activity_desc,
        start_time,
        end_time,
        create_time,
        condition_amount,
        condition_num,
        benefit_amount,
        benefit_discount,
        CASE rule.activity_type
            WHEN '3101' THEN CONCAT('满',condition_amount,'元减',benefit_amount,'元')
            WHEN '3102' THEN CONCAT('满',condition_num,'件打', benefit_discount,' 折')
            WHEN '3103' THEN CONCAT('打', benefit_discount,'折')
        END benefit_rule,
        benefit_level,
        '{partition_date}' AS ds  -- 直接在SQL中定义ds字段
    FROM
        (
            SELECT
                id,
                activity_id,
                activity_type,
                condition_amount,
                condition_num,
                benefit_amount,
                benefit_discount,
                benefit_level
            FROM ods_activity_rule
            WHERE dt='{partition_date}'
        ) rule
        LEFT JOIN
        (
            SELECT
                id,
                activity_name,
                activity_type,
                activity_desc,
                start_time,
                end_time,
                create_time
            FROM ods_activity_info
            WHERE dt='{partition_date}'
        ) info
        ON rule.activity_id = info.id
        LEFT JOIN
        (
            SELECT
                dic_code,
                dic_name
            FROM ods_base_dic
            WHERE dt='{partition_date}'
              AND parent_code='31'
        ) dic
        ON rule.activity_type = dic.dic_code;
    """

    print(f"[INFO] 开始执行SQL查询，目标分区：{partition_date}")
    df1 = spark.sql(select_sql1)

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df1.show(5)  # 直接显示df1，已包含ds字段

    # 写入数据（按ds分区）
    select_to_hive(df1, tableName)  # 直接传入df1，无需额外添加ds

    # 验证数据
    print(f"[INFO] 验证分区 {partition_date} 的数据...")
    verify_df = spark.sql(f"SELECT * FROM {tableName} WHERE dt='{partition_date}' LIMIT 5")
    verify_df.show()

if __name__ == "__main__":
    target_date = '20250701'
    execute_hive_insert(target_date, 'dim_activity_full')