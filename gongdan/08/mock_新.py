from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, concat, rand, round, col, expr, when, floor, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
import os

# 配置环境
os.environ["PYSPARK_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("Generate500MetricsData") \
    .getOrCreate()

# 定义英文列名schema
columns = [
    "id", "content_id", "content_type", "related_product_id", "date", "click_rate", "conversion_rate",
    "top50_rank", "exposure", "clicks", "watch_duration", "send_cnt", "pay_cnt", "pay_amt", "pay_buyers",
    "cum_send_cnt", "cum_pay_amt", "cum_pay_cnt", "redem_cnt", "send_amt", "active_act_cnt", "comp_act_cnt",
    "single_act_send_cnt", "single_act_pay_cnt", "single_act_pay_amt", "cs_send_cnt", "cs_redem_cnt", "cs_pay_amt",
    "prod_disc_send_cnt", "sku_disc_send_cnt", "fixed_disc_send_cnt", "custom_disc_send_cnt", "norm_final_prc",
    "min_final_prc", "est_group_prc_range", "per_person_limit", "disc_amt", "act_cover_prod_cnt", "act_cover_sku_cnt",
    "store_valid_act_cnt", "prod_valid_act_cnt", "total_act_prod_cnt"
]

# 生成500条基础数据
df = spark.range(500).selectExpr("id")

# 生成内容ID（C001-C500）
df = df.withColumn("content_id", concat(lit("C"), expr("lpad(id + 1, 3, '0')")))

# 内容类型（直播/图文/短视频）
content_types = ["直播", "图文", "短视频"]
df = df.withColumn(
    "content_type",
    when(floor(rand() * 3) == 0, lit(content_types[0]))
    .when(floor(rand() * 3) == 1, lit(content_types[1]))
    .otherwise(lit(content_types[2]))
).withColumn("content_type", regexp_replace(col("content_type"), " ", ""))  # 处理空格

# 关联商品ID（P006-P024）
df = df.withColumn("related_product_id", concat(lit("P"), expr("lpad(cast(rand() * 18 + 6 as int), 3, '0')")))

# 日期（2025-07-01至2025-07-10）
df = df.withColumn("date", expr("date_add('2025-07-01', cast(rand() * 10 as int))").cast(DateType()))

# 点击率（0.1-0.3）
df = df.withColumn("click_rate", round(rand() * 0.2 + 0.1, 4))

# 转化率（0.03-0.1）
df = df.withColumn("conversion_rate", round(rand() * 0.07 + 0.03, 4))

# TOP50排名（1-50）
df = df.withColumn("top50_rank", floor(rand() * 49 + 1).cast(IntegerType()))

# 曝光量（1000-20000）
df = df.withColumn("exposure", floor(rand() * 19000 + 1000).cast(IntegerType()))

# 点击量（曝光量的10%-30%）
df = df.withColumn("clicks", floor(col("exposure") * (rand() * 0.2 + 0.1)).cast(IntegerType()))

# 观看时长（100-600）
df = df.withColumn("watch_duration", floor(rand() * 500 + 100).cast(IntegerType()))

# 31个客服专属优惠指标（匹配示例格式）
df = df.withColumn("send_cnt", floor(rand() * 99 + 1).cast(IntegerType()))
df = df.withColumn("pay_cnt", floor(rand() * 49 + 1).cast(IntegerType()))
df = df.withColumn("pay_amt", round(rand() * 4900 + 100, 2))
df = df.withColumn("pay_buyers", floor(rand() * 19 + 1).cast(IntegerType()))
df = df.withColumn("cum_send_cnt", col("send_cnt") + floor(rand() * 50).cast(IntegerType()))
df = df.withColumn("cum_pay_amt", col("pay_amt") + round(rand() * 1000, 2))
df = df.withColumn("cum_pay_cnt", col("pay_cnt") + floor(rand() * 20).cast(IntegerType()))
df = df.withColumn("redem_cnt", floor(rand() * col("pay_cnt")).cast(IntegerType()))
df = df.withColumn("send_amt", round(col("pay_amt") + rand() * 200 - 100, 2))
df = df.withColumn("active_act_cnt", floor(rand() * 9 + 1).cast(IntegerType()))
df = df.withColumn("comp_act_cnt", floor(rand() * col("active_act_cnt")).cast(IntegerType()))
df = df.withColumn("single_act_send_cnt", floor(rand() * 49 + 1).cast(IntegerType()))
df = df.withColumn("single_act_pay_cnt", floor(rand() * 19 + 1).cast(IntegerType()))
df = df.withColumn("single_act_pay_amt", round(rand() * 1950 + 50, 2))
df = df.withColumn("cs_send_cnt", floor(rand() * col("send_cnt")).cast(IntegerType()))
df = df.withColumn("cs_redem_cnt", floor(rand() * col("redem_cnt")).cast(IntegerType()))
df = df.withColumn("cs_pay_amt", round(rand() * col("pay_amt"), 2))
df = df.withColumn("prod_disc_send_cnt", floor(rand() * 29 + 1).cast(IntegerType()))
df = df.withColumn("sku_disc_send_cnt", floor(rand() * 19 + 1).cast(IntegerType()))
df = df.withColumn("fixed_disc_send_cnt", floor(rand() * 14 + 1).cast(IntegerType()))
df = df.withColumn("custom_disc_send_cnt", floor(rand() * 9 + 1).cast(IntegerType()))
df = df.withColumn("norm_final_prc", round(rand() * 950 + 50, 2))
df = df.withColumn("min_final_prc", round(col("norm_final_prc") - rand() * 50, 2))
df = df.withColumn("est_group_prc_range",
                   concat(round(col("min_final_prc") - rand() * 50, 2).cast(StringType()),
                          lit("-"),
                          round(col("norm_final_prc") + rand() * 50, 2).cast(StringType()),
                          lit(" 元")))  # 保留示例中的空格格式
df = df.withColumn("per_person_limit", floor(rand() * 4 + 1).cast(IntegerType()))
df = df.withColumn("disc_amt", round(rand() * 490 + 10, 2))
df = df.withColumn("act_cover_prod_cnt", floor(rand() * 49 + 1).cast(IntegerType()))
df = df.withColumn("act_cover_sku_cnt", floor(rand() * 29 + 1).cast(IntegerType()))
df = df.withColumn("store_valid_act_cnt", floor(rand() * 9 + 1).cast(IntegerType()))
df = df.withColumn("prod_valid_act_cnt", floor(rand() * 4 + 1).cast(IntegerType()))
df = df.withColumn("total_act_prod_cnt", floor(rand() * 199 + 1).cast(IntegerType()))

# 调整列顺序与示例一致
df = df.select(columns)

# 保存为CSV（mock_data目录）
output_dir = "mock_data"
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, "mock")

df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .option("encoding", "UTF-8") \
    .csv(output_path)

spark.stop()
