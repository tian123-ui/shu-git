from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import os
import random
from datetime import datetime, timedelta
from builtins import round as python_round

# 配置PySpark环境
os.environ["PYSPARK_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("商品360看板全量DWD层生成") \
    .getOrCreate()

# ---------------------- 读取数据源 ----------------------
# 读取ODS层数据
ods_sales = spark.read.csv("mock_data/ods_sales_original", header=True, inferSchema=True)
ods_traffic = spark.read.csv("mock_data/ods_traffic_original", header=True, inferSchema=True)
ods_behavior = spark.read.csv("mock_data/ods_user_behavior_original", header=True, inferSchema=True)
ods_comment = spark.read.csv("mock_data/ods_comment_original", header=True, inferSchema=True)
ods_content = spark.read.csv("mock_data/ods_content_original", header=True, inferSchema=True)

# 读取DIM层数据
dim_platform = spark.read.csv("mock_data/dim_platform_channel", header=True, inferSchema=True)
dim_product = spark.read.csv("mock_data/dim_product_sku", header=True, inferSchema=True).alias("p")
dim_user = spark.read.csv("mock_data/dim_user", header=True, inferSchema=True)
dim_price = spark.read.csv("mock_data/dim_price_band", header=True, inferSchema=True).alias("pr")


# ---------------------- 1. 生成dwd_sku_sales_detail（SKU销售明细事实表） ----------------------
# 生成随机物流时间UDF
def generate_logistics_time(order_time):
    if not order_time:
        return None
    try:
        base_time = datetime.strptime(order_time, "%Y-%m-%d %H:%M:%S")
        delay = random.randint(1, 3)
        return (base_time + timedelta(days=delay)).strftime("%Y-%m-%d %H:%M:%S")
    except:
        return None


logistics_udf = udf(generate_logistics_time, StringType())


# 生成随机折扣率UDF
def random_discount():
    return python_round(random.uniform(0.85, 1.0), 2)


discount_udf = udf(random_discount, DoubleType())

# 计算热销指数（销量/类目平均销量）需要的窗口 - 明确指定使用商品表的所属类目
category_window = Window.partitionBy(col("p.所属类目"))

dwd_sku_sales_detail = ods_sales \
    .join(dim_product, ["商品ID", "SKU ID"], "left") \
    .join(dim_price, col("p.所属类目") == col("pr.所属类目"), "left") \
    .withColumn("日期", to_date(col("订单时间"))) \
    .withColumn("实际支付金额", col("订单金额") * discount_udf()) \
    .withColumn("物流时间", logistics_udf(col("支付时间"))) \
    .withColumn("类目平均销量", avg("销售数量").over(category_window)) \
    .withColumn("热销指数", round(col("销售数量") / col("类目平均销量"), 2)) \
    .withColumn("库存余量", round(rand() * 1000 + 100)) \
    .select(
    col("商品ID"),
    col("SKU ID"),
    col("SKU属性（颜色/尺寸）").alias("SKU属性"),
    col("日期"),
    col("销售数量"),
    col("实际支付金额").alias("销售金额"),
    col("热销指数"),
    col("库存余量"),
    col("p.所属类目").alias("所属类目"),  # 明确使用商品表的所属类目
    col("pr.价格区间").alias("价格区间")
)

dwd_sku_sales_detail.write.mode("overwrite").option("header", "true").csv("mock_data/dwd_sku_sales_detail")
print("dwd_sku_sales_detail生成完成")


# ---------------------- 2. 生成dwd_traffic_channel_detail（流量渠道明细事实表） ----------------------
# 生成随机会话时长UDF
def random_session_duration():
    return random.randint(60, 1800)


session_udf = udf(random_session_duration, IntegerType())

dwd_traffic_channel_detail = ods_traffic \
    .join(dim_platform,
          [ods_traffic["平台（platform）"] == dim_platform["平台名称"],
           ods_traffic["渠道（channel）"] == dim_platform["渠道名称"]],
          "left") \
    .withColumn("日期", to_date(col("访问时间"))) \
    .withColumn("加购数", when(col("转化行为（是否加购/下单）").contains("加购"), 1).otherwise(0)) \
    .withColumn("下单数", when(col("转化行为（是否加购/下单）").contains("下单"), 1).otherwise(0)) \
    .groupBy(
    col("平台名称"),
    col("渠道名称"),
    col("日期"),
    col("渠道类型")
) \
    .agg(
    countDistinct("访客ID").alias("访客数（UV）"),
    sum(when(col("点击行为（是否点击商品）") == "是", 1).otherwise(0)).alias("点击数"),
    sum("加购数").alias("加购数"),
    sum("下单数").alias("下单数")
) \
    .withColumn("渠道转化率", round(col("下单数") / col("访客数（UV）"), 4)) \
    .select(
    col("平台名称").alias("平台"),
    col("渠道名称").alias("渠道"),
    col("日期"),
    col("访客数（UV）"),
    col("点击数"),
    col("加购数"),
    col("下单数"),
    col("渠道转化率"),
    col("渠道类型")
)

dwd_traffic_channel_detail.write.mode("overwrite").option("header", "true").csv("mock_data/dwd_traffic_channel_detail")
print("dwd_traffic_channel_detail生成完成")


# ---------------------- 3. 生成dwd_title_keyword_detail（标题关键词明细事实表） ----------------------
# 提取标题词根并拆分
def split_keywords(keyword_str):
    if not keyword_str:
        return []
    return keyword_str.split("/")


split_keywords_udf = udf(split_keywords, ArrayType(StringType()))


# 生成词根分类UDF
def classify_keyword(keyword):
    categories = ["手机", "电脑", "服装", "鞋包"]
    modifiers = ["新款", "正品", "特价", "高端"]
    if keyword in categories:
        return "品类词"
    elif keyword in modifiers:
        return "修饰词"
    else:
        return "长尾词"


classify_udf = udf(classify_keyword, StringType())

dwd_title_keyword_detail = ods_behavior \
    .filter(col("标题词根点击记录").isNotNull()) \
    .withColumn("标题词根", explode(split_keywords_udf(col("标题词根点击记录")))) \
    .withColumn("词根类型", classify_udf(col("标题词根"))) \
    .withColumn("日期", to_date(col("加入购物车时间"))) \
    .withColumn("成交数", when(col("支付记录（商品ID/金额/时间）").isNotNull(), 1).otherwise(0)) \
    .groupBy(
    col("浏览商品ID").alias("商品ID"),
    col("标题词根"),
    col("词根类型"),
    col("日期")
) \
    .agg(
    count("*").alias("搜索次数"),
    countDistinct("用户ID").alias("引流人数（UV）"),
    sum("成交数").alias("成交数")
) \
    .withColumn("词根转化率", round(col("成交数") / col("引流人数（UV）"), 4)) \
    .select(
    col("商品ID"),
    col("标题词根"),
    col("搜索次数"),
    col("引流人数（UV）"),
    col("词根转化率"),
    col("词根类型"),
    col("日期")
)

dwd_title_keyword_detail.write.mode("overwrite").option("header", "true").csv("mock_data/dwd_title_keyword_detail")
print("dwd_title_keyword_detail生成完成")


# ---------------------- 4. 生成dwd_content_effect_detail（内容效果明细事实表） ----------------------
# 生成随机TOP50排名UDF
def random_rank():
    return random.randint(1, 50)


rank_udf = udf(random_rank, IntegerType())

dwd_content_effect_detail = ods_content \
    .withColumn("日期", to_date(col("发布时间"))) \
    .withColumn("点击率", round(col("点击量") / col("曝光量"), 4)) \
    .withColumn("转化率", round(col("转化量（通过内容下单数）") / col("点击量"), 4)) \
    .withColumn("TOP50排名", rank_udf()) \
    .select(
    col("内容ID"),
    col("内容类型（直播/短视频/图文）").alias("内容类型"),
    col("关联商品ID"),
    col("日期"),
    col("点击率"),
    col("转化率"),
    col("TOP50排名"),
    col("曝光量"),
    col("点击量"),
    col("观看时长")
)

dwd_content_effect_detail.write.mode("overwrite").option("header", "true").csv("mock_data/dwd_content_effect_detail")
print("dwd_content_effect_detail生成完成")


# ---------------------- 5. 生成dwd_rfm_behavior_detail（RFM行为明细事实表） ----------------------
# 解析支付记录提取金额
def extract_payment_amount(payment_str):
    if not payment_str:
        return 0.0
    parts = payment_str.split("/")
    return float(parts[1]) if len(parts) >= 2 else 0.0


extract_amount_udf = udf(extract_payment_amount, DoubleType())

dwd_rfm_behavior_detail = ods_behavior \
    .filter(col("支付记录（商品ID/金额/时间）").isNotNull()) \
    .withColumn("支付金额", extract_amount_udf(col("支付记录（商品ID/金额/时间）"))) \
    .withColumn("支付时间",
                regexp_extract(col("支付记录（商品ID/金额/时间）"), r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', 1)) \
    .select(
    col("用户ID"),
    col("浏览商品ID").alias("商品ID"),
    col("支付时间").alias("最近消费时间（recency）"),
    lit(1).alias("消费次数（frequency）"),
    col("支付金额").alias("累计消费金额（monetary）"),
    col("加入购物车时间").alias("访问时间")
) \
    .groupBy("用户ID", "商品ID") \
    .agg(
    max("最近消费时间（recency）").alias("最近消费时间（recency）"),
    sum("消费次数（frequency）").alias("消费次数（frequency）"),
    sum("累计消费金额（monetary）").alias("累计消费金额（monetary）"),
    max("访问时间").alias("访问时间")
)

dwd_rfm_behavior_detail.write.mode("overwrite").option("header", "true").csv("mock_data/dwd_rfm_behavior_detail")
print("dwd_rfm_behavior_detail生成完成")

spark.stop()
