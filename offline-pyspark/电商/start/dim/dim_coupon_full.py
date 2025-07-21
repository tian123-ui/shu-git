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

def execute_hive_insert(partition_date: str, tableName):
    """从源表查询数据并写入目标Hive分区表"""
    spark = get_spark_session()

    # 构建动态SQL查询，只添加dt列作为分区字段
    select_sql1 = f"""
    select
        id,
        coupon_name,
        coupon_type,
        coupon_dic.dic_name,
        condition_amount,
        condition_num,
        activity_id,
        benefit_amount,
        benefit_discount,
        case coupon_type
            when '3201' then concat('满',condition_amount,'元减',benefit_amount,'元')
            when '3202' then concat('满',condition_num,'件打', benefit_discount,' 折')
            when '3203' then concat('减',benefit_amount,'元')
        end benefit_rule,
        create_time,
        range_type,
        range_dic.dic_name,
        limit_num,
        taken_count,
        start_time,
        end_time,
        operate_time,
        expire_time,
        '{partition_date}' as dt  -- 生成表实际分区字段
    from
        (
            select
                id,
                coupon_name,
                coupon_type,
                condition_amount,
                condition_num,
                activity_id,
                benefit_amount,
                benefit_discount,
                create_time,
                range_type,
                limit_num,
                taken_count,
                start_time,
                end_time,
                operate_time,
                expire_time
            from ods_coupon_info
            where dt='{partition_date}'
        )ci
            left join
        (
            select
                dic_code,
                dic_name
            from ods_base_dic
            where dt='{partition_date}'
              and parent_code='32'
        )coupon_dic
        on ci.coupon_type=coupon_dic.dic_code
            left join
        (
            select
                dic_code,
                dic_name
            from ods_base_dic
            where dt='{partition_date}'
              and parent_code='33'
        )range_dic
        on ci.range_type=range_dic.dic_code;
    """

    print(f"[INFO] 开始执行SQL查询，目标分区：{partition_date}")
    df1 = spark.sql(select_sql1)

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df1.show(5)  # 显示不含ds列的DataFrame

    # 写入数据（移除添加ds列的操作）
    select_to_hive(df1, tableName)

    # 验证数据（假设表分区字段为dt）
    print(f"[INFO] 验证分区 {partition_date} 的数据...")
    verify_df = spark.sql(f"SELECT * FROM {tableName} WHERE dt='{partition_date}' LIMIT 5")
    verify_df.show()


if __name__ == "__main__":
    target_date = '20250701'
    execute_hive_insert(target_date, 'dim_coupon_full')