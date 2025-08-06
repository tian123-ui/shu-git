from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import os
import random  # 新增导入random模块


# 配置PySpark环境
os.environ["PYSPARK_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"


# 初始化SparkSession
spark = SparkSession.builder \
    .appName("商品360看板DIM层生成") \
    .getOrCreate()

# 读取ODS层数据（假设存储于mock_data目录）
ods_sales = spark.read.csv("mock_data/ods_sales_original", header=True, inferSchema=True)
ods_traffic = spark.read.csv("mock_data/ods_traffic_original", header=True, inferSchema=True)
ods_behavior = spark.read.csv("mock_data/ods_user_behavior_original", header=True, inferSchema=True)
ods_comment = spark.read.csv("mock_data/ods_comment_original", header=True, inferSchema=True)

# 1. 生成dim_platform_channel（平台渠道维度表）
# 优化窗口函数，增加分区减少警告
platform_window = Window.partitionBy(lit(1)).orderBy("平台名称")
channel_window = Window.partitionBy(lit(1)).orderBy("渠道名称")

dim_platform_channel = ods_traffic.select(
    col("平台（platform）").alias("平台名称"),
    col("渠道（channel）").alias("渠道名称")
).dropDuplicates() \
.withColumn("平台ID", concat(lit("PT"), row_number().over(platform_window).cast("string"))) \
.withColumn("渠道ID", concat(lit("CH"), row_number().over(channel_window).cast("string"))) \
.withColumn("渠道类型",
    when(col("渠道名称").isin("App Store", "华为应用市场", "官网"), "官方")
    .otherwise("第三方")
) \
.select("平台ID", "平台名称", "渠道ID", "渠道名称", "渠道类型")

dim_platform_channel.write.mode("overwrite").option("header", "true").csv("mock_data/dim_platform_channel")
print("dim_platform_channel生成完成")

# 2. 生成dim_product_sku（商品SKU维度表）
# 提取SKU属性中的材质信息（假设从SKU ID解析）
extract_material = udf(lambda x: x.split("-")[-1] if len(x.split("-"))>2 else "未知", StringType())

dim_product_sku = ods_sales.select(
    col("商品ID"),
    col("SKU ID"),
    col("SKU属性（颜色/尺寸）").alias("SKU属性")
).dropDuplicates() \
.withColumn("商品名称", concat(lit("2025夏季_"), col("商品ID"))) \
.withColumn("SKU属性", concat(col("SKU属性"), lit(";材质:"), extract_material(col("SKU ID")))) \
.withColumn("所属类目", lit("女装")) \
.withColumn("子类目", lit("连衣裙")) \
.withColumn("上架时间", lit("2025-06-01")) \
.select("商品ID", "商品名称", "SKU ID", "SKU属性", "所属类目", "子类目", "上架时间")

dim_product_sku.write.mode("overwrite").option("header", "true").csv("mock_data/dim_product_sku")
print("dim_product_sku生成完成")

# 3. 生成dim_user（用户维度表）
# 生成随机注册时间的UDF，避免单值重复
def random_reg_date():
    month = random.randint(1, 6)
    day = random.randint(1, 28)
    return f"2025-{month:02d}-{day:02d}"

random_date_udf = udf(random_reg_date, StringType())

dim_user = ods_behavior.select(col("用户ID")).dropDuplicates() \
.union(ods_comment.select(col("评价用户ID（是否老买家）").substr(1, 4).alias("用户ID")).dropDuplicates()) \
.dropDuplicates() \
.withColumn("注册时间", random_date_udf())  \
.withColumn("用户标签",
    when(col("用户ID").substr(2, 3).cast("int") % 4 == 0, "新客").otherwise("老客")
) \
.withColumn("会员等级",
    when(col("用户ID").substr(2, 3).cast("int") % 5 == 0, "黄金")
    .when(col("用户ID").substr(2, 3).cast("int") % 3 == 0, "白银")
    .otherwise("普通")
)

dim_user.write.mode("overwrite").option("header", "true").csv("mock_data/dim_user")
print("dim_user生成完成")

# 4. 生成dim_price_band（价格带维度表）
dim_price_band = spark.createDataFrame([
    ("PB01", "0-50元", "女装"),
    ("PB02", "51-100元", "女装"),
    ("PB03", "101-200元", "女装"),
    ("PB04", "201-500元", "女装"),
    ("PB05", "501元以上", "女装")
], ["价格带ID", "价格区间", "所属类目"])

dim_price_band.write.mode("overwrite").option("header", "true").csv("mock_data/dim_price_band")
print("dim_price_band生成完成")

spark.stop()
