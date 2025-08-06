from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import lit, col, to_timestamp
from datetime import datetime, timedelta
import random
import os

# 配置PySpark环境
os.environ["PYSPARK_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("商品360看板ODS层纯PySpark生成") \
    .getOrCreate()

# 创建输出目录
output_dir = "mock_data"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# 基础参数配置
start_date = datetime(2025, 7, 1)
end_date = datetime(2025, 7, 10)
date_list = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end_date - start_date).days + 1)]
product_ids = [f"P{i:03d}" for i in range(1, 21)]
user_ids = [f"U{i:03d}" for i in range(1, 1001)]
sku_attrs = [("红", "S"), ("蓝", "M"), ("黑", "L"), ("白", "XL")]
platforms = ["iOS", "Android", "Web"]
channels = {
    "iOS": ["App Store", "TestFlight", "Enterprise"],
    "Android": ["华为应用市场", "小米应用商店", "OPPO应用商店"],
    "Web": ["官网", "搜索引擎", "社交媒体"]
}
content_types = ["直播", "短视频", "图文"]
search_terms = ["夏季连衣裙", "休闲裤", "雪纺上衣", "白色T恤", "红色短裙"]
entry_pages = ["首页推荐", "搜索结果页", "分类页", "详情页", "首页轮播"]

# 1. 生成ods_sales_original（销售原始数据）
sales_schema = StructType([
    StructField("订单时间", StringType(), True),
    StructField("商品ID", StringType(), True),
    StructField("SKU ID", StringType(), True),
    StructField("SKU属性（颜色/尺寸）", StringType(), True),
    StructField("销售数量", IntegerType(), True),
    StructField("单价", DoubleType(), True),
    StructField("订单金额", DoubleType(), True),
    StructField("购买用户ID", StringType(), True),
    StructField("支付时间", StringType(), True),
    StructField("退款状态", StringType(), True)
])

sales_data = []
for date in date_list:
    for pid in product_ids[:5]:
        for color, size in sku_attrs:
            sku_id = f"{pid}-{color[0]}-{size}"
            qty = random.randint(1, 10)
            price = round(random.uniform(99.9, 499.9), 2)
            amount = round(qty * price, 2)
            order_time = f"{date} {random.randint(9, 22)}:{random.randint(0, 59)}:{random.randint(0, 59)}"
            pay_delay = random.randint(0, 5)
            pay_time = (datetime.strptime(order_time, "%Y-%m-%d %H:%M:%S") + timedelta(minutes=pay_delay)).strftime("%Y-%m-%d %H:%M:%S")
            refund_status = "未退款" if random.random() < 0.95 else "已退款"
            sales_data.append((order_time, pid, sku_id, f"颜色:{color};尺寸:{size}", qty, price, amount, random.choice(user_ids), pay_time, refund_status))

spark.createDataFrame(sales_data, sales_schema) \
    .coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv(f"{output_dir}/ods_sales_original")
print("ods_sales_original生成完成，路径：mock_data/ods_sales_original")

# 2. 生成ods_traffic_original（流量原始数据）
traffic_schema = StructType([
    StructField("平台（platform）", StringType(), True),
    StructField("渠道（channel）", StringType(), True),
    StructField("访客ID", StringType(), True),
    StructField("访问时间", StringType(), True),
    StructField("点击行为（是否点击商品）", StringType(), True),
    StructField("转化行为（是否加购/下单）", StringType(), True),
    StructField("入口页面", StringType(), True)
])

traffic_data = []
for date in date_list:
    for plat in platforms:
        for chan in channels[plat]:
            for _ in range(random.randint(100, 200)):
                visitor_id = f"V{random.randint(10000, 99999)}"
                visit_time = f"{date} {random.randint(8, 23)}:{random.randint(0, 59)}:{random.randint(0, 59)}"
                click_behavior = "是" if random.random() < 0.3 else "否"
                conversion_behavior = random.choices(["加购", "下单", "无"], weights=[0.2, 0.1, 0.7], k=1)[0] if click_behavior == "是" else "无"
                traffic_data.append((plat, chan, visitor_id, visit_time, click_behavior, conversion_behavior, random.choice(entry_pages)))

spark.createDataFrame(traffic_data, traffic_schema) \
    .coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv(f"{output_dir}/ods_traffic_original")
print("ods_traffic_original生成完成，路径：mock_data/ods_traffic_original")

# 3. 生成ods_user_behavior_original（用户行为原始数据）
behavior_schema = StructType([
    StructField("用户ID", StringType(), True),
    StructField("搜索词", StringType(), True),
    StructField("访问路径（页面序列）", StringType(), True),
    StructField("停留时长", IntegerType(), True),
    StructField("浏览商品ID", StringType(), True),
    StructField("加入购物车时间", StringType(), True),
    StructField("支付记录（商品ID/金额/时间）", StringType(), True),
    StructField("标题词根点击记录", StringType(), True)
])

behavior_data = []
for date in date_list[:3]:
    for uid in user_ids[:50]:
        search_term = random.choice(search_terms)
        pid = random.choice(product_ids)
        path = random.choice([f"首页→搜索页→{pid}详情页", f"首页→分类页→{pid}详情页"])
        add_cart_time = f"{date} {random.randint(10, 21)}:{random.randint(0, 59)}:{random.randint(0, 59)}"
        pay_record = ""
        if random.random() < 0.3:
            pay_amount = round(random.uniform(99.9, 499.9), 2)
            pay_time = (datetime.strptime(add_cart_time, "%Y-%m-%d %H:%M:%S") + timedelta(minutes=random.randint(1, 10))).strftime("%Y-%m-%d %H:%M:%S")
            pay_record = f"{pid}/{pay_amount}/{pay_time}"
        keywords = search_term.split(" ")
        title_keywords = "|".join(keywords) if len(keywords) > 1 else f"{keywords[0]}|新款"
        behavior_data.append((uid, search_term, path, random.randint(60, 600), pid, add_cart_time, pay_record, title_keywords))

spark.createDataFrame(behavior_data, behavior_schema) \
    .coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv(f"{output_dir}/ods_user_behavior_original")
print("ods_user_behavior_original生成完成，路径：mock_data/ods_user_behavior_original")

# 4. 生成ods_content_original（内容原始数据）
content_schema = StructType([
    StructField("内容ID", StringType(), True),
    StructField("内容类型（直播/短视频/图文）", StringType(), True),
    StructField("发布时间", StringType(), True),
    StructField("关联商品ID", StringType(), True),
    StructField("曝光量", IntegerType(), True),
    StructField("点击量", IntegerType(), True),
    StructField("观看时长", IntegerType(), True),
    StructField("转化量（通过内容下单数）", IntegerType(), True)
])

content_data = []
for i in range(1, 51):
    ctype = random.choice(content_types)
    publish_time = f"{random.choice(date_list)} {random.randint(10, 22)}:{random.randint(0, 59)}:{random.randint(0, 59)}"
    related_pid = random.choice(product_ids)
    exposure = random.randint(5000, 20000)
    clicks = round(exposure * random.uniform(0.1, 0.3))
    content_data.append((f"C{i:03d}", ctype, publish_time, related_pid, exposure, clicks, random.randint(30, 600), round(clicks * random.uniform(0.02, 0.1))))

spark.createDataFrame(content_data, content_schema) \
    .coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv(f"{output_dir}/ods_content_original")
print("ods_content_original生成完成，路径：mock_data/ods_content_original")

# 5. 生成ods_comment_original（评价原始数据）
comment_schema = StructType([
    StructField("评价ID", StringType(), True),
    StructField("商品ID", StringType(), True),
    StructField("SKU ID", StringType(), True),
    StructField("评价用户ID（是否老买家）", StringType(), True),
    StructField("评分（1-5星）", IntegerType(), True),
    StructField("评价内容", StringType(), True),
    StructField("评价时间", StringType(), True),
    StructField("是否主动评价", StringType(), True)
])

comments = ["质量很好，推荐购买", "尺寸合适，满意", "一般般，符合预期", "不太满意，材质一般", "非常好，会回购", "颜色和图片一致"]
comment_data = []
for i in range(1, 101):
    pid = random.choice(product_ids)
    color, size = random.choice(sku_attrs)
    sku_id = f"{pid}-{color[0]}-{size}"
    uid = random.choice(user_ids)
    is_old = "老买家" if random.random() < 0.6 else "新买家"
    comment_data.append((f"CM{i:03d}", pid, sku_id, f"{uid}({is_old})", random.randint(1, 5), random.choice(comments), f"{random.choice(date_list)} {random.randint(10, 23)}:{random.randint(0, 59)}:{random.randint(0, 59)}", "是" if random.random() < 0.4 else "否"))

spark.createDataFrame(comment_data, comment_schema) \
    .coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv(f"{output_dir}/ods_comment_original")
print("ods_comment_original生成完成，路径：mock_data/ods_comment_original")

spark.stop()
print("所有ODS层CSV表生成完成，均存储于mock_data目录下")