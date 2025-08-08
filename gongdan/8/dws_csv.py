import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType, DateType
from pyspark.sql.functions import col, count, sum, round, current_timestamp, to_date, when, lit, datediff

# 工单编号：大数据 - 电商数仓 - 08 - 商品主题营销工具客服专属优惠看板


# 配置PySpark环境
os.environ["PYSPARK_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("生成DWS汇总数据层表") \
    .master("local[*]") \
    .getOrCreate()

# 创建存储目录
output_root = "mock_data/dws"
dwd_root = "mock_data"
dim_root = "mock_data"

# 创建必要的目录
for dir_path in [output_root, dwd_root, dim_root]:
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)


# ------------------------------
# 生成模拟数据并保存为CSV文件
# ------------------------------

# 生成模拟的客服维度数据
def generate_dim_customer_service():
    data = {
        "customer_service_id": [f"cs_{i:03d}" for i in range(1, 11)],
        "cs_name": [f"客服{i}" for i in range(1, 11)],
        "shop_id": [f"shop_{i % 3 + 1}" for i in range(1, 11)]
    }
    df = pd.DataFrame(data)
    df.to_csv(f"{dim_root}/dim_customer_service.csv", index=False)
    return df


# 生成模拟的店铺维度数据
def generate_dim_shop():
    data = {
        "shop_id": ["shop_1", "shop_2", "shop_3"],
        "shop_name": ["旗舰店", "专营店", "专卖店"],
        "platform": ["淘宝", "京东", "拼多多"]
    }
    df = pd.DataFrame(data)
    df.to_csv(f"{dim_root}/dim_shop.csv", index=False)
    return df


# 生成模拟的活动数据
def generate_dwd_activity():
    data = {
        "activity_id": [f"act_{i:03d}" for i in range(1, 6)],
        "activity_name": [f"活动{i}" for i in range(1, 6)],
        "activity_level": ["A", "B", "A", "C", "B"],
        "promo_type": ["满减", "折扣", "满减", "赠品", "折扣"],
        "status": ["进行中", "已结束", "进行中", "未开始", "进行中"]
    }
    df = pd.DataFrame(data)
    df.to_csv(f"{dwd_root}/dwd_customer_service_promo_activity.csv", index=False)
    return df


# 生成模拟的发送数据
def generate_dwd_send():
    np.random.seed(42)
    dates = [(datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d %H:%M:%S") for i in range(30)]

    data = {
        "send_id": [f"send_{i:05d}" for i in range(1, 1001)],
        "customer_service_id": [f"cs_{np.random.randint(1, 11):03d}" for _ in range(1000)],
        "activity_id": [f"act_{np.random.randint(1, 6):03d}" for _ in range(1000)],
        "product_id": [f"prod_{np.random.randint(1, 21)}" for _ in range(1000)],
        "product_name": [f"商品{np.random.randint(1, 21)}" for _ in range(1000)],
        "sku_id": [f"sku_{np.random.randint(1, 51)}" if np.random.random() > 0.3 else None for _ in range(1000)],
        "sku_spec": [f"{np.random.choice(['红色', '蓝色', '黑色'])}-{np.random.choice(['S', 'M', 'L', 'XL'])}"
                     if np.random.random() > 0.3 else None for _ in range(1000)],
        "send_time": [np.random.choice(dates) for _ in range(1000)]
    }
    df = pd.DataFrame(data)
    df.to_csv(f"{dwd_root}/dwd_customer_service_promo_send.csv", index=False)
    return df


# 生成模拟的核销数据
def generate_dwd_use(send_df):
    np.random.seed(43)
    # 随机选择30%的发送记录进行核销
    used_send_ids = np.random.choice(send_df["send_id"], size=int(len(send_df) * 0.3), replace=False)

    dates = [(datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d %H:%M:%S") for i in range(15)]

    data = {
        "use_id": [f"use_{i:05d}" for i in range(1, len(used_send_ids) + 1)],
        "send_id": used_send_ids,
        "pay_time": [np.random.choice(dates) for _ in range(len(used_send_ids))],
        "is_valid_use": [np.random.choice(["true", "false"], p=[0.9, 0.1]) for _ in range(len(used_send_ids))]
    }
    df = pd.DataFrame(data)
    df.to_csv(f"{dwd_root}/dwd_customer_service_promo_use.csv", index=False)
    return df


# 生成所有模拟数据
dim_cs_df = generate_dim_customer_service()
dim_shop_df = generate_dim_shop()
activity_df = generate_dwd_activity()
send_df = generate_dwd_send()
use_df = generate_dwd_use(send_df)

# ------------------------------
# 1. 每日汇总表：dws_customer_service_promo_daily
# 维度：日期+店铺 | 指标：发送/核销核心数据
# ------------------------------
dws_daily_schema = StructType([
    StructField("stat_date", DateType(), False),
    StructField("shop_id", StringType(), False),
    StructField("shop_name", StringType(), False),
    StructField("platform", StringType(), False),
    StructField("total_send_count", IntegerType(), False),
    StructField("total_use_count", IntegerType(), False),
    StructField("valid_use_count", IntegerType(), False),
    StructField("use_rate", DecimalType(5, 2), False),
    StructField("etl_update_time", TimestampType(), False)
])

# 读取DWD发送表（使用正确路径）
dwd_send = spark.read \
    .option("header", "true") \
    .csv(f"{dwd_root}/dwd_customer_service_promo_send.csv")

# 读取DWD核销表
dwd_use = spark.read \
    .option("header", "true") \
    .csv(f"{dwd_root}/dwd_customer_service_promo_use.csv")

# 读取店铺维度表
dim_shop = spark.read \
    .option("header", "true") \
    .csv(f"{dim_root}/dim_shop.csv")

# 读取客服维度表（用于获取shop_id）
dim_cs = spark.read \
    .option("header", "true") \
    .csv(f"{dim_root}/dim_customer_service.csv")

# 关键修复：dwd_send关联dim_cs获取shop_id
dwd_send_with_shop = dwd_send.join(
    dim_cs.select("customer_service_id", "shop_id"),
    on="customer_service_id",
    how="left"
)

# 按日期+店铺聚合（使用带shop_id的发送表）
daily_agg = dwd_send_with_shop \
    .withColumn("stat_date", to_date(col("send_time"))) \
    .groupBy("stat_date", "shop_id") \
    .agg(count("send_id").alias("total_send_count")) \
    .join(
    # 核销表也需要关联客服表获取shop_id
    dwd_use.join(dwd_send.select("send_id", "customer_service_id"), on="send_id", how="left")
    .join(dim_cs.select("customer_service_id", "shop_id"), on="customer_service_id", how="left")
    .withColumn("stat_date", to_date(col("pay_time")))
    .groupBy("stat_date", "shop_id")
    .agg(
        count("use_id").alias("total_use_count"),
        sum(when(col("is_valid_use") == "true", 1).otherwise(0)).alias("valid_use_count")
    ),
    on=["stat_date", "shop_id"],
    how="full_outer"
) \
    .join(dim_shop.select("shop_id", "shop_name", "platform"), on="shop_id", how="left") \
    .fillna(0, subset=["total_send_count", "total_use_count", "valid_use_count"]) \
    .withColumn("use_rate",
                when(col("total_send_count") == 0, 0)
                .otherwise(round(col("total_use_count") / col("total_send_count") * 100, 2))
                ) \
    .withColumn("etl_update_time", current_timestamp())

dws_daily = daily_agg.select(
    "stat_date", "shop_id", "shop_name", "platform",
    "total_send_count", "total_use_count", "valid_use_count",
    "use_rate", "etl_update_time"
)

dws_daily.write.mode("overwrite").option("header", "true").csv(f"{output_root}/dws_customer_service_promo_daily")

# ------------------------------
# 2. 活动汇总表：dws_customer_service_promo_activity
# 维度：活动 | 指标：活动效果数据
# ------------------------------
dws_activity_schema = StructType([
    StructField("activity_id", StringType(), False),
    StructField("activity_name", StringType(), False),
    StructField("activity_level", StringType(), False),
    StructField("promo_type", StringType(), False),
    StructField("activity_status", StringType(), False),
    StructField("total_send_count", IntegerType(), False),
    StructField("total_use_count", IntegerType(), False),
    StructField("7d_send_count", IntegerType(), False),
    StructField("7d_use_count", IntegerType(), False),
    StructField("use_rate", DecimalType(5, 2), False),
    StructField("etl_update_time", TimestampType(), False)
])

# 读取DWD活动表
dwd_activity = spark.read \
    .option("header", "true") \
    .csv(f"{dwd_root}/dwd_customer_service_promo_activity.csv")

# 活动发送数据聚合（使用带shop_id的发送表）
activity_send = dwd_send_with_shop \
    .withColumn("is_7d", datediff(current_timestamp(), to_date(col("send_time"))) <= 7) \
    .groupBy("activity_id") \
    .agg(
    count("send_id").alias("total_send_count"),
    sum(when(col("is_7d"), 1).otherwise(0)).alias("7d_send_count")
)

# 活动核销数据聚合 - 修复：明确指定activity_id来源，避免重复
activity_use = dwd_use \
    .join(
    dwd_send_with_shop.select("send_id", "activity_id", "send_time"),
    on="send_id",
    how="left"
) \
    .select(
    col("send_id"),
    col("activity_id"),  # 明确使用发送表的activity_id
    col("pay_time"),
    col("use_id"),
    col("send_time")
) \
    .withColumn("is_7d", datediff(current_timestamp(), to_date(col("pay_time"))) <= 7) \
    .groupBy("activity_id") \
    .agg(
    count("use_id").alias("total_use_count"),
    sum(when(col("is_7d"), 1).otherwise(0)).alias("7d_use_count")
)

# 关联活动维度信息
dws_activity = dwd_activity \
    .join(activity_send, on="activity_id", how="left") \
    .join(activity_use, on="activity_id", how="left") \
    .fillna(0, subset=["total_send_count", "total_use_count", "7d_send_count", "7d_use_count"]) \
    .withColumn("use_rate",
                when(col("total_send_count") == 0, 0)
                .otherwise(round(col("total_use_count") / col("total_send_count") * 100, 2))
                ) \
    .withColumnRenamed("status", "activity_status") \
    .withColumn("etl_update_time", current_timestamp()) \
    .select(
    "activity_id", "activity_name", "activity_level", "promo_type",
    "activity_status", "total_send_count", "total_use_count",
    "7d_send_count", "7d_use_count", "use_rate", "etl_update_time"
)

dws_activity.write.mode("overwrite").option("header", "true").csv(f"{output_root}/dws_customer_service_promo_activity")

# ------------------------------
# 3. 客服汇总表：dws_customer_service_promo_cs
# 维度：客服+周期 | 指标：客服绩效数据
# ------------------------------
dws_cs_schema = StructType([
    StructField("customer_service_id", StringType(), False),
    StructField("cs_name", StringType(), False),
    StructField("shop_id", StringType(), False),
    StructField("stat_period", StringType(), False),
    StructField("send_count", IntegerType(), False),
    StructField("use_count", IntegerType(), False),
    StructField("use_rate", DecimalType(5, 2), False),
    StructField("etl_update_time", TimestampType(), False)
])

# 多周期聚合（日/7天/30天）
periods = [("day", 1), ("7days", 7), ("30days", 30)]
cs_dfs = []
for period_name, days in periods:
    cs_agg = dwd_send_with_shop \
        .withColumn("in_period", datediff(current_timestamp(), to_date(col("send_time"))) <= days) \
        .groupBy("customer_service_id") \
        .agg(sum(when(col("in_period"), 1).otherwise(0)).alias("send_count")) \
        .join(
        dwd_use.join(dwd_send_with_shop.select("send_id", "customer_service_id", "send_time"), on="send_id", how="left")
        .withColumn("in_period", datediff(current_timestamp(), to_date(col("pay_time"))) <= days)
        .groupBy("customer_service_id")
        .agg(sum(when(col("in_period"), 1).otherwise(0)).alias("use_count")),
        on="customer_service_id",
        how="left"
    ) \
        .join(dim_cs, on="customer_service_id", how="left") \
        .fillna(0, subset=["send_count", "use_count"]) \
        .withColumn("use_rate",
                    when(col("send_count") == 0, 0)
                    .otherwise(round(col("use_count") / col("send_count") * 100, 2))
                    ) \
        .withColumn("stat_period", lit(period_name)) \
        .withColumn("etl_update_time", current_timestamp()) \
        .select(
            "customer_service_id",
            "cs_name",
            "shop_id",
            "stat_period",
            "send_count",
            "use_count",
            "use_rate",
            "etl_update_time"
        )

    cs_dfs.append(cs_agg)

# 合并多周期数据（此时字段顺序已与schema一致，无需再指定schema）
dws_cs = spark.createDataFrame(
    spark.sparkContext.union([df.rdd for df in cs_dfs])
).select(
    "customer_service_id", "cs_name", "shop_id", "stat_period",
    "send_count", "use_count", "use_rate", "etl_update_time"
)

dws_cs.write.mode("overwrite").option("header", "true").csv(f"{output_root}/dws_customer_service_promo_cs")


# ------------------------------
# 4. 商品汇总表：dws_customer_service_promo_product
# 维度：商品/SKU | 指标：商品优惠效果
# ------------------------------
dws_product_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("sku_id", StringType(), True),
    StructField("sku_spec", StringType(), True),
    StructField("total_send_count", IntegerType(), False),
    StructField("total_use_count", IntegerType(), False),
    StructField("use_rate", DecimalType(5, 2), False),
    StructField("etl_update_time", TimestampType(), False)
])

# 商品/sku维度聚合
product_agg = dwd_send_with_shop \
    .groupBy("product_id", "product_name", "sku_id", "sku_spec") \
    .agg(count("send_id").alias("total_send_count")) \
    .join(
    dwd_use.join(dwd_send_with_shop.select("send_id", "product_id", "product_name", "sku_id", "sku_spec"), on="send_id",
                 how="left")
    .groupBy("product_id", "product_name", "sku_id", "sku_spec")
    .agg(count("use_id").alias("total_use_count")),
    on=["product_id", "product_name", "sku_id", "sku_spec"],
    how="full_outer"
) \
    .fillna(0, subset=["total_send_count", "total_use_count"]) \
    .withColumn("use_rate",
                when(col("total_send_count") == 0, 0)
                .otherwise(round(col("total_use_count") / col("total_send_count") * 100, 2))
                ) \
    .withColumn("etl_update_time", current_timestamp())

dws_product = product_agg.select(
    "product_id", "product_name", "sku_id", "sku_spec",
    "total_send_count", "total_use_count", "use_rate", "etl_update_time"
)

dws_product.write.mode("overwrite").option("header", "true").csv(f"{output_root}/dws_customer_service_promo_product")

print(f"DWS汇总数据层表已生成至：{os.path.abspath(output_root)}，共4张表")
spark.stop()



# 你提供的 DWS 层代码符合《大数据 - 电商数仓 - 08 - 商品主题营销工具看板 V1.2-20250125.pdf》中客服专属优惠业务的汇总需求，主要体现在以下几个方面：
#
# 业务规则适配
# 时间维度支持 “日 / 7 天 / 30 天”，与文档中看板的时间筛选要求一致，可满足不同周期的数据分析需求🔶2-30。
# 活动汇总表区分 “进行中 / 已结束” 等状态，对应文档中 “活动效果展示不同状态下单个活动效果数据” 的要求🔶2-29。
# 核销率计算逻辑覆盖跨天场景，符合 “优惠使用周期 24 小时可能导致支付次数大于发送次数” 的业务特点🔶2-41。
# 数据流转合理性
# 通过关联 DWD 层明细数据（发送表、核销表）和 DIM 层维度数据（店铺、客服），实现了从明细到汇总的分层加工，符合 “ODS→DIM→DWD→DWS” 的数仓流转逻辑。
# 关键修复（如dwd_send关联dim_cs获取shop_id）解决了表间关联断层问题，确保店铺维度的汇总指标准确。
# 指标设计贴合看板需求
# 包含 “发送次数”“核销次数”“核销率” 等核心指标，直接对应文档中 “工具效果总览”“活动效果”“客服数据” 等模块的展示内容🔶2-32🔶2-34🔶2-36。
# 商品汇总表按 “商品 / SKU” 维度聚合，适配文档中 “商品级 / SKU 级优惠” 的业务划分，可支撑不同粒度的商品优惠效果分析🔶2-77🔶2-79。
#
# 代码生成的 4 张 DWS 表以 CSV 格式存储于mock_data/dws目录，与上层表存储风格一致，为后续 ADS 层的看板展示提供了预聚合的指标数据。

