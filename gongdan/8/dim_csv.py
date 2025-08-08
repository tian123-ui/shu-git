from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from pyspark.sql.functions import col, to_timestamp, regexp_replace, current_timestamp, when, substring, concat, lit
import os
from datetime import datetime

# 工单编号：大数据 - 电商数仓 - 08 - 商品主题营销工具客服专属优惠看板

# 配置PySpark环境
os.environ["PYSPARK_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("生成DIM维度表层表") \
    .master("local[*]") \
    .getOrCreate()

# 创建存储目录
output_root = "mock_data/dim"
if not os.path.exists(output_root):
    os.makedirs(output_root)
ods_root = "mock_data"  # ODS层数据路径（直接指向mock_data目录）

# ------------------------------
# 1. 商品维度表：dim_product
# 来源：ods_product.csv
# ------------------------------
product_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("price", DecimalType(10, 2), False),
    StructField("shop_id", StringType(), False),
    StructField("status", StringType(), False),  # 标准化为：onsale/offsale
    StructField("etl_update_time", TimestampType(), False)
])

# 读取ODS层商品表（修正路径，直接读取CSV文件）
ods_product = spark.read \
    .option("header", "true") \
    .schema("product_id STRING, product_name STRING, price DECIMAL(10,2), shop_id STRING, status STRING") \
    .csv(f"{ods_root}/ods_product.csv")  # 读取mock_data下的ods_product.csv

# 修复状态转换逻辑
dim_product = ods_product \
    .withColumn("status",
                when(col("status") == "在售", "onsale")
                .when(col("status") == "下架", "offsale")
                .otherwise("unknown")) \
    .withColumn("etl_update_time", current_timestamp()) \
    .select("product_id", "product_name", "price", "shop_id", "status", "etl_update_time")

# 写入DIM层
dim_product.write.mode("overwrite").option("header", "true").csv(f"{output_root}/dim_product")

# ------------------------------
# 2. SKU维度表：dim_sku
# 来源：ods_sku.csv + dim_product（关联商品信息）
# ------------------------------
sku_schema = StructType([
    StructField("sku_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),  # 关联商品名称
    StructField("spec", StringType(), False),
    StructField("sku_price", DecimalType(10, 2), False),
    StructField("etl_update_time", TimestampType(), False)
])

# 读取ODS层SKU表（修正路径）
ods_sku = spark.read \
    .option("header", "true") \
    .schema("sku_id STRING, product_id STRING, spec STRING, sku_price DECIMAL(10,2)") \
    .csv(f"{ods_root}/ods_sku.csv")  # 读取mock_data下的ods_sku.csv

# 关联商品维度表补充商品名称
dim_sku = ods_sku.join(
    dim_product.select("product_id", "product_name"),
    on="product_id",
    how="left"
).withColumn("etl_update_time", current_timestamp()) \
    .select("sku_id", "product_id", "product_name", "spec", "sku_price", "etl_update_time")

# 写入DIM层
dim_sku.write.mode("overwrite").option("header", "true").csv(f"{output_root}/dim_sku")

# ------------------------------
# 3. 客服维度表：dim_customer_service
# 来源：ods_customer_service.csv
# ------------------------------
cs_schema = StructType([
    StructField("customer_service_id", StringType(), False),
    StructField("cs_name", StringType(), False),
    StructField("shop_id", StringType(), False),
    StructField("etl_update_time", TimestampType(), False)
])

# 读取ODS层客服表（修正路径）
ods_cs = spark.read \
    .option("header", "true") \
    .schema("customer_service_id STRING, cs_name STRING, shop_id STRING") \
    .csv(f"{ods_root}/ods_customer_service.csv")  # 读取mock_data下的ods_customer_service.csv

dim_cs = ods_cs \
    .withColumn("cs_name", regexp_replace(col("cs_name"), "[^a-zA-Z0-9\u4e00-\u9fa5]", "")) \
    .withColumn("etl_update_time", current_timestamp()) \
    .select("customer_service_id", "cs_name", "shop_id", "etl_update_time")

# 写入DIM层
dim_cs.write.mode("overwrite").option("header", "true").csv(f"{output_root}/dim_customer_service")

# ------------------------------
# 4. 店铺维度表：dim_shop
# 来源：ODS层各表提取
# ------------------------------
shop_schema = StructType([
    StructField("shop_id", StringType(), False),
    StructField("shop_name", StringType(), False),  # 模拟店铺名称
    StructField("platform", StringType(), False),  # 淘宝/天猫
    StructField("etl_update_time", TimestampType(), False)
])

# 从商品表提取唯一店铺ID并补充信息
shop_data = ods_product.select("shop_id").distinct() \
    .withColumn("shop_name", concat(substring(col("shop_id"), 5, 10), lit("店铺"))) \
    .withColumn("platform",
                when(col("shop_id").like("%tmall%"), "tmall")
                .when(col("shop_id").like("%taobao%"), "taobao")
                .otherwise("unknown")) \
    .withColumn("etl_update_time", current_timestamp())

dim_shop = spark.createDataFrame(shop_data.rdd, schema=shop_schema)

# 写入DIM层
dim_shop.write.mode("overwrite").option("header", "true").csv(f"{output_root}/dim_shop")

# ------------------------------
# 5. 活动类型维度表：dim_activity_type
# ------------------------------
activity_type_schema = StructType([
    StructField("type_code", StringType(), False),
    StructField("type_name", StringType(), False),
    StructField("type_category", StringType(), False),  # 活动级别/优惠类型
    StructField("description", StringType(), True),
    StructField("etl_update_time", TimestampType(), False)
])

# 获取当前时间戳作为固定值（而不是Column对象）
current_time = datetime.now()

# 手动录入维度数据（使用实际时间戳值而非Column对象）
activity_type_data = [
    ("product_level", "商品级", "activity_level", "同一商品下所有SKU同价", current_time),
    ("sku_level", "SKU级", "activity_level", "同一商品下不同SKU可设不同价", current_time),
    ("fixed", "固定优惠", "promo_type", "优惠金额固定", current_time),
    ("custom", "自定义优惠", "promo_type", "客服可在上限内自由填写金额", current_time)
]

dim_activity_type = spark.createDataFrame(activity_type_data, schema=activity_type_schema)

# 写入DIM层
dim_activity_type.write.mode("overwrite").option("header", "true").csv(f"{output_root}/dim_activity_type")

print(f"DIM维度表层表已生成至：{os.path.abspath(output_root)}，共5张表")
spark.stop()


# 你提供的 DIM 层代码符合《大数据 - 电商数仓 - 08 - 商品主题营销工具看板 V1.2-20250125.pdf》中对客服专属优惠业务的维度数据要求，主要体现在以下几个方面：
#
# 维度表设计适配业务规则
# 商品维度表（dim_product）对商品状态进行标准化（“在售”→“onsale”、“下架”→“offsale”），与文档中商品参与优惠活动的状态管理逻辑一致🔶3-92。
# SKU 维度表（dim_sku）关联商品名称，清晰区分 “商品级” 与 “SKU 级” 优惠的适用范围，符合文档中活动级别的划分规则🔶3-77🔶3-79。
# 活动类型维度表（dim_activity_type）明确 “固定优惠”“自定义优惠” 的编码与描述，对应文档中优惠类型的核心配置项🔶3-86🔶3-88。
# 数据清洗逻辑贴合业务需求
# 客服维度表（dim_customer_service）去除姓名中的特殊字符，确保客服信息规范，适配后续客服数据统计场景🔶3-36。
# 店铺维度表（dim_shop）区分 “淘宝”“天猫” 平台，与文档中 “适用范围：淘宝 & 天猫商家” 的业务限定一致🔶3-55。
# 与 ODS 层流转衔接合理
# 所有维度表均基于 ODS 层原始数据加工生成，保留product_id“shop_id等核心关联字段，为后续 DWD 层明细事实表的关联分析提供可靠维度支撑，符合数仓分层流转逻辑。
#
# 代码生成的 5 张 DIM 表可有效支撑客服专属优惠业务的多维度分析，如按商品类型、客服、平台等维度拆解优惠活动效果。

