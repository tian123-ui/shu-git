from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType, BooleanType
from pyspark.sql.functions import col, to_timestamp, when, current_timestamp, expr
import os
import pandas as pd
from datetime import datetime, timedelta
import random

# 工单编号：大数据 - 电商数仓 - 08 - 商品主题营销工具客服专属优惠看板

# 配置PySpark环境
os.environ["PYSPARK_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("生成DWD明细数据层表") \
    .master("local[*]") \
    .getOrCreate()

# 创建存储目录
output_root = "mock_data/dwd"
ods_root = "mock_data"  # ODS层路径
dim_root = "mock_data"  # DIM层路径

# 创建所有需要的目录
for dir_path in [output_root, ods_root, dim_root]:
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)


# 生成模拟数据（如果不存在）
def generate_mock_data():
    """生成必要的模拟数据用于测试"""
    # 生成店铺维度表
    if not os.path.exists(f"{dim_root}/dim_shop.csv"):
        data = {
            "shop_id": ["shop_001", "shop_002", "shop_003"],
            "shop_name": ["旗舰店", "专营店", "折扣店"],
            "platform": ["淘宝", "京东", "拼多多"],
            "etl_update_time": [datetime.now() for _ in range(3)]
        }
        pd.DataFrame(data).to_csv(f"{dim_root}/dim_shop.csv", index=False)

    # 生成商品维度表
    if not os.path.exists(f"{dim_root}/dim_product.csv"):
        data = {
            "product_id": ["prod_001", "prod_002", "prod_003", "prod_004"],
            "product_name": ["手机", "电脑", "平板", "耳机"],
            "price": [5999.99, 7999.99, 3999.99, 1299.99],
            "shop_id": ["shop_001", "shop_001", "shop_002", "shop_003"],
            "status": ["在售", "在售", "在售", "下架"],
            "etl_update_time": [datetime.now() for _ in range(4)]
        }
        pd.DataFrame(data).to_csv(f"{dim_root}/dim_product.csv", index=False)

    # 生成SKU维度表
    if not os.path.exists(f"{dim_root}/dim_sku.csv"):
        data = {
            "sku_id": ["sku_001", "sku_002", "sku_003", "sku_004"],
            "product_id": ["prod_001", "prod_001", "prod_002", "prod_003"],
            "product_name": ["手机", "手机", "电脑", "平板"],
            "spec": ["128G 黑色", "256G 白色", "512G 灰色", "256G 银色"],
            "sku_price": [5999.99, 6499.99, 8999.99, 4299.99],
            "etl_update_time": [datetime.now() for _ in range(4)]
        }
        pd.DataFrame(data).to_csv(f"{dim_root}/dim_sku.csv", index=False)

    # 生成客服维度表
    if not os.path.exists(f"{dim_root}/dim_customer_service.csv"):
        data = {
            "customer_service_id": ["cs_001", "cs_002", "cs_003"],
            "cs_name": ["张三", "李四", "王五"],
            "shop_id": ["shop_001", "shop_002", "shop_001"],
            "etl_update_time": [datetime.now() for _ in range(3)]
        }
        pd.DataFrame(data).to_csv(f"{dim_root}/dim_customer_service.csv", index=False)

    # 生成活动表
    if not os.path.exists(f"{ods_root}/ods_customer_service_promo_activity.csv"):
        data = {
            "activity_id": ["act_001", "act_002", "act_003"],
            "activity_name": ["618大促", "店庆活动", "新品推广"],
            "activity_level": ["商品级", "SKU级", "商品级"],
            "promo_type": ["固定优惠", "自定义优惠", "固定优惠"],
            "custom_promo_max": [100, 200, None],
            "start_time": [(datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S"),
                           (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S"),
                           datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
            "end_time": [(datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S"),
                         (datetime.now() + timedelta(days=10)).strftime("%Y-%m-%d %H:%M:%S"),
                         (datetime.now() + timedelta(days=15)).strftime("%Y-%m-%d %H:%M:%S")],
            "status": ["进行中", "进行中", "未开始"],
            "shop_id": ["shop_001", "shop_002", "shop_001"],
            "create_time": [(datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d %H:%M:%S"),
                            (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S"),
                            (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")],
            "raw_extra": ["", "", ""]
        }
        pd.DataFrame(data).to_csv(f"{ods_root}/ods_customer_service_promo_activity.csv", index=False)

    # 生成活动-商品关联表
    if not os.path.exists(f"{ods_root}/ods_customer_service_promo_activity_item.csv"):
        data = {
            "id": ["item_001", "item_002", "item_003", "item_004"],
            "activity_id": ["act_001", "act_001", "act_002", "act_003"],
            "product_id": ["prod_001", "prod_002", "prod_003", "prod_001"],
            "sku_id": ["sku_001", None, "sku_003", "sku_002"],
            "promo_amount": [500, 800, 300, 600],
            "limit_purchase_count": [5, 3, 10, 2],
            "is_removed": ["否", "否", "否", "是"],
            "create_time": [(datetime.now() - timedelta(days=9)).strftime("%Y-%m-%d %H:%M:%S"),
                            (datetime.now() - timedelta(days=9)).strftime("%Y-%m-%d %H:%M:%S"),
                            (datetime.now() - timedelta(days=4)).strftime("%Y-%m-%d %H:%M:%S"),
                            (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")]
        }
        pd.DataFrame(data).to_csv(f"{ods_root}/ods_customer_service_promo_activity_item.csv", index=False)

    # 生成优惠发送表
    if not os.path.exists(f"{ods_root}/ods_customer_service_promo_send.csv"):
        data = {
            "send_id": ["send_001", "send_002", "send_003", "send_004"],
            "activity_id": ["act_001", "act_001", "act_002", "act_001"],
            "product_id": ["prod_001", "prod_002", "prod_003", "prod_001"],
            "sku_id": ["sku_001", None, "sku_003", "sku_002"],
            "customer_service_id": ["cs_001", "cs_001", "cs_002", "cs_003"],
            "consumer_id": ["user_001", "user_002", "user_003", "user_004"],
            "actual_promo_amount": [500, 800, 300, 600],
            "valid_duration": [24, 12, 8, 4],
            "send_time": [(datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S"),
                          (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
                          (datetime.now() - timedelta(hours=12)).strftime("%Y-%m-%d %H:%M:%S"),
                          (datetime.now() - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")],
            "remark": ["新用户优惠", "", "老客户回馈", "限时优惠"]
        }
        pd.DataFrame(data).to_csv(f"{ods_root}/ods_customer_service_promo_send.csv", index=False)

    # 生成优惠核销表
    if not os.path.exists(f"{ods_root}/ods_customer_service_promo_use.csv"):
        data = {
            "use_id": ["use_001", "use_002", "use_003"],
            "send_id": ["send_001", "send_002", "send_003"],
            "order_id": ["order_001", "order_002", "order_003"],
            "pay_time": [(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
                         (datetime.now() - timedelta(hours=10)).strftime("%Y-%m-%d %H:%M:%S"),
                         (datetime.now() - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")],
            "pay_amount": [5499.99, 7199.99, 3699.99],
            "purchase_count": [1, 1, 2]
        }
        pd.DataFrame(data).to_csv(f"{ods_root}/ods_customer_service_promo_use.csv", index=False)


# 生成模拟数据（如果不存在）
generate_mock_data()

# ------------------------------
# 1. 活动明细事实表：dwd_customer_service_promo_activity
# 来源：ods_customer_service_promo_activity + dim_shop
# ------------------------------
# 读取ODS层活动表
ods_activity = spark.read \
    .option("header", "true") \
    .schema("""
        activity_id STRING, activity_name STRING, activity_level STRING, promo_type STRING,
        custom_promo_max INT, start_time STRING, end_time STRING, status STRING,
        shop_id STRING, create_time STRING, raw_extra STRING
    """) \
    .csv(f"{ods_root}/ods_customer_service_promo_activity.csv")

# 读取DIM层店铺表
dim_shop = spark.read \
    .option("header", "true") \
    .schema("shop_id STRING, shop_name STRING, platform STRING, etl_update_time TIMESTAMP") \
    .csv(f"{dim_root}/dim_shop.csv")

# 数据清洗与转换
dwd_activity = ods_activity \
    .withColumn("activity_level",
                when(col("activity_level") == "商品级", "product_level")
                .when(col("activity_level") == "SKU级", "sku_level")
                .otherwise("unknown")) \
    .withColumn("promo_type",
                when(col("promo_type") == "固定优惠", "fixed")
                .when(col("promo_type") == "自定义优惠", "custom")
                .otherwise("unknown")) \
    .withColumn("start_time", to_timestamp(col("start_time"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("end_time", to_timestamp(col("end_time"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("create_time", to_timestamp(col("create_time"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("status",
                when(col("status") == "未开始", "not_started")
                .when(col("status") == "进行中", "ongoing")
                .when(col("status") == "已结束", "ended")
                .otherwise("unknown")) \
    .join(dim_shop.select("shop_id", "shop_name"), on="shop_id", how="left") \
    .withColumn("etl_load_time", current_timestamp()) \
    .select("activity_id", "activity_name", "activity_level", "promo_type",
            "custom_promo_max", "start_time", "end_time", "status",
            "shop_id", "shop_name", "create_time", "etl_load_time")

# 写入DWD层
dwd_activity.write.mode("overwrite").option("header", "true").csv(f"{output_root}/dwd_customer_service_promo_activity")

# ------------------------------
# 2. 活动-商品关联明细事实表：dwd_customer_service_promo_activity_item
# 来源：ods_customer_service_promo_activity_item + dim_product + dim_sku
# ------------------------------
# 读取ODS层活动-商品关联表
ods_item = spark.read \
    .option("header", "true") \
    .schema("""
        id STRING, activity_id STRING, product_id STRING, sku_id STRING,
        promo_amount INT, limit_purchase_count INT, is_removed STRING, create_time STRING
    """) \
    .csv(f"{ods_root}/ods_customer_service_promo_activity_item.csv")

# 读取DIM层商品表和SKU表
dim_product = spark.read \
    .option("header", "true") \
    .schema(
    "product_id STRING, product_name STRING, price DECIMAL(10,2), shop_id STRING, status STRING, etl_update_time TIMESTAMP") \
    .csv(f"{dim_root}/dim_product.csv")

dim_sku = spark.read \
    .option("header", "true") \
    .schema(
    "sku_id STRING, product_id STRING, product_name STRING, spec STRING, sku_price DECIMAL(10,2), etl_update_time TIMESTAMP") \
    .csv(f"{dim_root}/dim_sku.csv")

# 数据清洗与转换
dwd_item = ods_item \
    .withColumn("is_removed", when(col("is_removed") == "是", True).otherwise(False)) \
    .withColumn("create_time", to_timestamp(col("create_time"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("promo_amount",
                when(col("promo_amount") > 5000, 5000)  # 超出上限截断
                .otherwise(col("promo_amount"))) \
    .join(dim_product.select("product_id", "product_name"), on="product_id", how="left") \
    .join(dim_sku.select("sku_id", "spec").withColumnRenamed("spec", "sku_spec"),
          on="sku_id", how="left") \
    .withColumn("etl_load_time", current_timestamp()) \
    .select("id", "activity_id", "product_id", "product_name",
            "sku_id", "sku_spec", "promo_amount", "limit_purchase_count",
            "is_removed", "create_time", "etl_load_time")

# 写入DWD层
dwd_item.write.mode("overwrite").option("header", "true").csv(f"{output_root}/dwd_customer_service_promo_activity_item")

# ------------------------------
# 3. 优惠发送明细事实表：dwd_customer_service_promo_send
# 来源：ods_customer_service_promo_send + dim_customer_service + dim_product
# ------------------------------
# 读取ODS层优惠发送表
ods_send = spark.read \
    .option("header", "true") \
    .schema("""
        send_id STRING, activity_id STRING, product_id STRING, sku_id STRING,
        customer_service_id STRING, consumer_id STRING, actual_promo_amount INT,
        valid_duration INT, send_time STRING, remark STRING
    """) \
    .csv(f"{ods_root}/ods_customer_service_promo_send.csv")

# 读取DIM层客服表
dim_cs = spark.read \
    .option("header", "true") \
    .schema("customer_service_id STRING, cs_name STRING, shop_id STRING, etl_update_time TIMESTAMP") \
    .csv(f"{dim_root}/dim_customer_service.csv")

# 数据清洗与转换
dwd_send = ods_send \
    .withColumn("send_time", to_timestamp(col("send_time"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("expire_time", expr("send_time + make_interval(0, 0, 0, 0, valid_duration, 0, 0)")) \
    .withColumn("valid_duration",
                when(col("valid_duration") < 1, 1)
                .when(col("valid_duration") > 24, 24)
                .otherwise(col("valid_duration"))) \
    .withColumn("is_expired", current_timestamp() > col("expire_time")) \
    .join(dim_cs.select("customer_service_id", "cs_name"), on="customer_service_id", how="left") \
    .join(dim_product.select("product_id", "product_name"), on="product_id", how="left") \
    .withColumn("etl_load_time", current_timestamp()) \
    .select("send_id", "activity_id", "product_id", "product_name",
            "sku_id", "customer_service_id", "cs_name", "consumer_id",
            "actual_promo_amount", "valid_duration", "send_time", "expire_time",
            "remark", "is_expired", "etl_load_time")

# 写入DWD层
dwd_send.write.mode("overwrite").option("header", "true").csv(f"{output_root}/dwd_customer_service_promo_send")

# ------------------------------
# 4. 优惠核销明细事实表：dwd_customer_service_promo_use
# 来源：ods_customer_service_promo_use + dwd_customer_service_promo_send
# ------------------------------
# 读取ODS层优惠核销表
ods_use = spark.read \
    .option("header", "true") \
    .schema("""
        use_id STRING, send_id STRING, order_id STRING, pay_time STRING,
        pay_amount DECIMAL(10,2), purchase_count INT
    """) \
    .csv(f"{ods_root}/ods_customer_service_promo_use.csv")

# 数据清洗与转换
dwd_use = ods_use \
    .withColumn("pay_time", to_timestamp(col("pay_time"), "yyyy-MM-dd HH:mm:ss")) \
    .join(dwd_send.select("send_id", "activity_id", "product_id", "consumer_id", "expire_time"),
          on="send_id", how="left") \
    .withColumn("is_valid_use", col("pay_time") <= col("expire_time")) \
    .withColumn("etl_load_time", current_timestamp()) \
    .select("use_id", "send_id", "activity_id", "order_id",
            "product_id", "consumer_id", "pay_time", "pay_amount",
            "purchase_count", "is_valid_use", "etl_load_time")

# 写入DWD层
dwd_use.write.mode("overwrite").option("header", "true").csv(f"{output_root}/dwd_customer_service_promo_use")

print(f"DWD明细数据层表已生成至：{os.path.abspath(output_root)}，共4张表")
spark.stop()



# 你提供的 DWD 层代码符合《大数据 - 电商数仓 - 08 - 商品主题营销工具看板 V1.2-20250125.pdf》中客服专属优惠业务的明细数据处理需求，主要体现在以下几个方面：
#
# 业务规则严格适配
# 活动级别标准化为 “product_level”“sku_level”，对应文档中 “商品级 & SKU 级优惠” 的划分🔶3-77🔶3-79；优惠类型转换为 “fixed”“custom”，匹配 “固定优惠 & 自定义优惠” 的业务配置🔶3-86🔶3-88。
# 优惠金额校验逻辑（超出 5000 元截断）符合文档中 “优惠金额不超过 5000 元” 的约束🔶3-99🔶3-100；有效期限制在 1-24 小时，与 “消费者可使用效期由客服自定义填写（24 小时以内）” 的规则一致🔶3-81🔶3-206。
# 核销有效性判断（is_valid_use）基于 “支付时间≤过期时间”，解决了文档中 “优惠使用周期 24 小时导致支付次数可能大于发送次数” 的跨天场景🔶3-41。
# 数据清洗逻辑贴合业务场景
# 时间字段统一转换为TimestampType，确保活动起止时间、发送时间、支付时间等的时间维度分析一致性。
# 关联 DIM 层补充商品名称、店铺名称、客服姓名等描述性信息，使明细数据既能支撑指标计算，又能满足业务人员对 “谁、在什么活动中、发送了什么商品优惠” 的追溯需求🔶3-36🔶3-37。
# 活动状态标准化为 “not_started”“ongoing”“ended”，与文档中活动管理的 “未开始 / 进行中 / 已结束” 状态流转逻辑对应🔶3-122🔶3-130。
# 表间关联完整覆盖业务链路
# 通过activity_id关联活动与商品，send_id关联发送与核销记录，完整保留从 “活动创建→商品关联→优惠发送→消费者核销” 的全链路数据痕迹，为后续 DWS 层按活动、客服、商品等维度汇总指标提供了细粒度的事实依据🔶3-29🔶3-34。
#
# 代码生成的 4 张 DWD 表可直接支撑客服专属优惠活动效果的明细分析，如单条优惠发送的核销情况、活动关联商品的优惠力度分布等，为上层汇总提供了可靠的明细数据基础。
