from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, round, when, lit
import os

# 配置 Spark 运行环境（关联 Anaconda 环境）
os.environ["PYSPARK_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"


def init_spark():
    """初始化 SparkSession，配置本地模式"""
    return SparkSession.builder \
       .appName("Product360Dashboard") \
       .config("spark.driver.memory", "4g") \
       .config("spark.executor.memory", "4g") \
       .master("local[*]") \
       .getOrCreate()


def load_data(spark, data_path):
    """读取原始 CSV 数据，自动推断 Schema"""
    return spark.read \
       .format("csv") \
       .option("header", "true") \
       .option("inferSchema", "true") \
       .load(data_path)


def calculate_ads_bu(df):
    """
    计算不分层指标（ads_bu 表）：
    按 dt、platform、channel 聚合，覆盖用户规模、营收、留存、行为指标
    """
    return df.groupBy("date", "platform", "channel") \
       .agg(
            # 1. 用户规模指标
            sum("new_users").alias("new_users"),
            sum("active_users").alias("active_users"),
            sum("paying_users").alias("paying_users"),

            # 2. 营收转化指标
            sum("total_revenue").alias("total_revenue"),
            round(sum("total_revenue") / sum("active_users"), 2).alias("arpu"),
            round(sum("total_revenue") / sum("paying_users"), 2).alias("arppu"),
            round(sum("paying_users") / sum("active_users"), 4).alias("conversion_rate"),

            # 3. 留存指标
            avg("retention_rate_1d").alias("retention_rate_1d"),
            avg("retention_rate_7d").alias("retention_rate_7d"),
            avg("retention_rate_30d").alias("retention_rate_30d"),

            # 4. 用户行为指标
            sum("session_count").alias("session_count"),
            avg("average_session_duration").alias("avg_session_duration"),
            sum("page_views").alias("page_views"),
            round(sum("page_views") / sum("session_count"), 2).alias("avg_page_views"),
            avg("bounce_rate").alias("bounce_rate")
        ) \
       .withColumnRenamed("date", "dt")  # 统一日期字段为 dt


def calculate_ads_rfm(df):
    """
    计算 RFM 分层指标（ads_rfm 表）：
    1. 先按 rfm_total 分层（高/中/低价值）
    2. 再按 dt、platform、channel、rfm_layer 聚合
    """
    # 分层规则：rfm_total ≥12=高价值，≥8=中等，否则低价值
    df_with_layer = df.withColumn(
        "rfm_layer",
        when(col("rfm_total") >= 12, "高价值用户")
       .when(col("rfm_total") >= 8, "中等价值用户")
       .otherwise("低价值用户")
    )

    return df_with_layer.groupBy("date", "platform", "channel", "rfm_layer") \
       .agg(
            # 1. 用户规模指标（与 ads_bu 一致）
            sum("new_users").alias("new_users"),
            sum("active_users").alias("active_users"),
            sum("paying_users").alias("paying_users"),

            # 2. 营收转化指标（与 ads_bu 一致）
            sum("total_revenue").alias("total_revenue"),
            round(sum("total_revenue") / sum("active_users"), 2).alias("arpu"),
            round(sum("total_revenue") / sum("paying_users"), 2).alias("arppu"),
            round(sum("paying_users") / sum("active_users"), 4).alias("conversion_rate"),

            # 3. 留存指标（与 ads_bu 一致）
            avg("retention_rate_1d").alias("retention_rate_1d"),
            avg("retention_rate_7d").alias("retention_rate_7d"),

            # 4. 用户行为指标（与 ads_bu 一致）
            sum("session_count").alias("session_count"),
            avg("bounce_rate").alias("bounce_rate")
        ) \
       .withColumnRenamed("date", "dt")  # 统一日期字段为 dt


def save_csv(df, output_dir, table_name):
    """
    保存为 CSV 文件：
    1. 合并分区为 1 个文件（coalesce(1)）
    2. 覆盖旧文件，确保目录干净
    """
    # 确保输出目录存在
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 完整路径
    full_path = os.path.join(output_dir, f"{table_name}.csv")

    # 删除旧文件（如果存在）
    if os.path.exists(full_path):
        os.remove(full_path)

    # 写入 CSV（合并分区）
    df.coalesce(1) \
        .write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(output_dir)

    # Spark 会生成 part-00000 文件，手动重命名为目标文件名
    part_file = [f for f in os.listdir(output_dir) if f.startswith("part-")][0]
    os.rename(os.path.join(output_dir, part_file), full_path)

    # 清理临时文件（如 .crc 文件）
    for f in os.listdir(output_dir):
        if f.endswith(".crc"):
            os.remove(os.path.join(output_dir, f))

    print(f"✅ 表 [{table_name}] 已保存至：{full_path}")


def main():
    # 配置路径
    data_path = "D:/Pycharm1/day1/工单/03-商品主题-商品360看板/1.生成数据/metrics_data_with_rfm/mock_with_rfm.csv"
    output_dir = "ads_bu_360"  # 输出目录
    output_dir1 = "ads_rfm_360"  # 输出目录

    # 初始化 Spark
    spark = init_spark()

    # 读取原始数据
    raw_df = load_data(spark, data_path)
    print(f"🔹 原始数据加载完成，共 {raw_df.count()} 行")

    # 1. 计算不分层指标（ads_bu）
    df_ads_bu = calculate_ads_bu(raw_df)
    save_csv(df_ads_bu, output_dir, "ads_bu")

    # 2. 计算 RFM 分层指标（ads_rfm）
    df_ads_rfm = calculate_ads_rfm(raw_df)
    save_csv(df_ads_rfm, output_dir1, "ads_rfm")

    # 预览结果（示例）
    print("\n📊 不分层指标示例（ads_bu）：")
    df_ads_bu.select("dt", "platform", "channel", "new_users", "total_revenue").show(3)

    print("\n📊 RFM 分层指标示例（ads_rfm）：")
    df_ads_rfm.select("dt", "platform", "channel", "rfm_layer", "active_users").show(3)

    # 停止 Spark
    spark.stop()


if __name__ == "__main__":
    main()