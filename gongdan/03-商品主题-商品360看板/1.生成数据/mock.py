from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, sum, count, max, datediff, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import random
from datetime import datetime, timedelta
import os
import shutil
import glob

import os
import shutil
import glob

# 替换为你的Anaconda环境python.exe的实际路径
os.environ["PYSPARK_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"


# 初始化 SparkSession，构建 Spark 应用入口
spark = SparkSession.builder \
    .appName("RFM-Product360") \
    .getOrCreate()  # 创建或获取 SparkSession

# 数据生成部分（复用之前的逻辑，新增 RFM 分层后处理）
# -------------------------  数据生成  -------------------------
random.seed(42)  # 设置随机种子，保证数据可复现
end_date = datetime.now()  # 获取当前时间作为数据生成的结束日期
start_date = end_date - timedelta(days=30)  # 计算数据生成的开始日期，过去 30 天
# 生成过去 30 天的日期列表，格式为 'YYYY - MM - DD'
date_list = [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(31)]

# 定义数据schema，即数据的结构和字段类型
schema = StructType([
    StructField("date", DateType(), True),  # 日期字段
    StructField("platform", StringType(), True),  # 平台字段
    StructField("channel", StringType(), True),  # 渠道字段
    StructField("new_users", IntegerType(), True),  # 新增用户数
    StructField("active_users", IntegerType(), True),  # 活跃用户数
    StructField("paying_users", IntegerType(), True),  # 付费用户数
    StructField("total_revenue", DoubleType(), True),  # 总营收
    StructField("average_revenue_per_user", DoubleType(), True),  # 人均营收
    StructField("average_revenue_per_paying_user", DoubleType(), True),  # 付费用户人均营收
    StructField("conversion_rate", DoubleType(), True),  # 转化率
    StructField("retention_rate_1d", DoubleType(), True),  # 1日留存率
    StructField("retention_rate_7d", DoubleType(), True),  # 7日留存率
    StructField("retention_rate_30d", DoubleType(), True),  # 30日留存率
    StructField("user_duration", DoubleType(), True),  # 用户时长(分钟)
    StructField("session_count", IntegerType(), True),  # 会话次数
    StructField("average_session_duration", DoubleType(), True),  # 平均会话时长(分钟)
    StructField("page_views", IntegerType(), True),  # 页面浏览量
    StructField("average_page_views", DoubleType(), True),  # 平均页面浏览量
    StructField("bounce_rate", DoubleType(), True),  # 跳出率
    StructField("error_rate", DoubleType(), True)  # 错误率
])

# 定义平台和对应的渠道列表
platforms = ["iOS", "Android", "Web"]
channels = {
    "iOS": ["App Store", "TestFlight", "Enterprise"],
    "Android": ["Google Play", "华为应用市场", "小米应用商店", "OPPO应用商店"],
    "Web": ["官网", "搜索引擎", "社交媒体", "第三方网站"]
}
# 定义平台营收系数，用于模拟不同平台付费能力差异
platform_revenue_factors = {"iOS": 1.2, "Android": 1.0, "Web": 0.8}

data_list = []
for date_str in date_list:
    date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()  # 将日期字符串转换为date对象
    for platform in platforms:
        for channel in channels[platform]:
            # 基础用户数（根据平台和日期生成有趋势的数据）
            base_users = random.randint(1000, 5000)
            day_factor = (datetime.strptime(date_str, '%Y-%m-%d') - start_date).days / 30  # 0到1的因子，用于模拟增长
            platform_factor = 1.2 if platform == "iOS" else 1.0 if platform == "Android" else 0.8

            new_users = int(base_users * platform_factor * (1 + day_factor * 0.5) * (0.8 + random.random() * 0.4))
            active_users = int(new_users * (1.5 + random.random() * 2.5))  # 活跃用户通常多于新增用户

            # 付费相关指标（结合平台付费能力系数调整）
            pay_rate = random.uniform(0.02, 0.15)  # 付费率2%-15%
            paying_users = int(active_users * pay_rate)
            arppu_base = random.uniform(50, 200)  # 付费用户平均收入基数
            # 用平台付费能力系数调整营收
            total_revenue = pay_rate * arppu_base * active_users * (0.8 + random.random() * 0.4) * \
                            platform_revenue_factors[platform]

            # 计算平均值指标
            arpu = total_revenue / active_users if active_users > 0 else 0  # 人均收入
            arppu = total_revenue / paying_users if paying_users > 0 else 0  # 付费用户人均收入

            # 转化和留存指标（修复留存率衰减逻辑：30d < 7d < 1d）
            conversion_rate = random.uniform(0.05, 0.3)  # 转化率5%-30%
            retention_1d = random.uniform(0.2, 0.6)  # 1日留存20%-60%
            # 7日留存 = 1日留存 * 衰减系数（0.3~0.7，但保证 7d < 1d）
            retention_7d = retention_1d * random.uniform(0.3, 0.7)
            # 30日留存 = 7日留存 * 衰减系数（0.2~0.6，保证 30d < 7d）
            retention_30d = retention_7d * random.uniform(0.2, 0.6)

            # 用户行为指标
            user_duration = random.uniform(5, 40)  # 用户时长5-40分钟
            session_count = int(active_users * random.uniform(1.2, 3.5))  # 会话次数
            avg_session_duration = random.uniform(2, 15)  # 平均会话时长2-15分钟

            # 页面浏览指标
            page_views = int(session_count * random.uniform(2, 10))  # 页面浏览量
            avg_page_views = page_views / session_count if session_count > 0 else 0  # 平均页面浏览量

            # 其他指标
            bounce_rate = random.uniform(0.1, 0.4)  # 跳出率10%-40%
            error_rate = random.uniform(0.001, 0.05)  # 错误率0.1%-5%

            # 四舍五入保留适当的小数位数
            row_data = (
                date_obj,  # 使用datetime对象而不是字符串
                platform,
                channel,
                new_users,
                active_users,
                paying_users,
                round(total_revenue, 2),
                round(arpu, 2),
                round(arppu, 2),
                round(conversion_rate, 4),
                round(retention_1d, 4),
                round(retention_7d, 4),
                round(retention_30d, 4),
                round(user_duration, 2),
                session_count,
                round(avg_session_duration, 2),
                page_views,
                round(avg_page_views, 2),
                round(bounce_rate, 4),
                round(error_rate, 4)
            )

            data_list.append(row_data)  # 将生成的行数据加入列表

# 一次性创建DataFrame
df = spark.createDataFrame(data_list, schema)


# -------------------------  数据生成结束  -------------------------


### 关键新增：RFM 分层与商品策略关联 ###
def add_rfm_layers(df):
    """
    1. 计算 RFM 核心指标（Recency, Frequency, Monetary）
    2. 分层（如 1-5 星，5 星为高价值）
    3. 关联商品策略（如高价值客户推荐高端品、普通客户推优惠）
    """
    # 假设以 "2025-08-05" 为分析截止日期（可动态获取）
    analysis_date = datetime(2025, 8, 5).date()

    # Step 1: 计算 R/F/M（需按客户聚合，这里简化为按平台/渠道聚合演示，实际需客户ID）
    # 注意：真实场景需有 `customer_id`，此处用 `platform-channel` 模拟客户分层
    rfm_df = df.groupBy("platform", "channel") \
        .agg(
        # Recency: 最近购买间隔（这里用最近日期距离分析日期的天数）
        datediff(lit(analysis_date), max("date")).alias("recency"),
        # Frequency: 购买频率（这里用会话次数模拟）
        avg("session_count").alias("frequency"),
        # Monetary: 总消费（直接用 total_revenue 聚合）
        sum("total_revenue").alias("monetary")
    )

    # Step 2: R/F/M 分层（分位数法，1-5 层，5 为最高价值）
    # 计算分位数（可动态调整分层规则）
    r_quantiles = rfm_df.approxQuantile("recency", [0.2, 0.4, 0.6, 0.8], 0.01)  # 越小越优（最近购买）
    f_quantiles = rfm_df.approxQuantile("frequency", [0.2, 0.4, 0.6, 0.8], 0.01)  # 越大越优
    m_quantiles = rfm_df.approxQuantile("monetary", [0.2, 0.4, 0.6, 0.8], 0.01)  # 越大越优

    # 定义分层逻辑（UDF 或 when-otherwise）
    def r_score(recency):
        if recency <= r_quantiles[0]:
            return 5
        elif recency <= r_quantiles[1]:
            return 4
        elif recency <= r_quantiles[2]:
            return 3
        elif recency <= r_quantiles[3]:
            return 2
        else:
            return 1

    def f_score(frequency):
        if frequency >= f_quantiles[3]:
            return 5
        elif frequency >= f_quantiles[2]:
            return 4
        elif frequency >= f_quantiles[1]:
            return 3
        elif frequency >= f_quantiles[0]:
            return 2
        else:
            return 1

    def m_score(monetary):
        if monetary >= m_quantiles[3]:
            return 5
        elif monetary >= m_quantiles[2]:
            return 4
        elif monetary >= m_quantiles[1]:
            return 3
        elif monetary >= m_quantiles[0]:
            return 2
        else:
            return 1

    # 注册UDF（或用 when 表达式）
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType

    r_udf = udf(r_score, IntegerType())
    f_udf = udf(f_score, IntegerType())
    m_udf = udf(m_score, IntegerType())

    rfm_scored = rfm_df \
        .withColumn("r_score", r_udf("recency")) \
        .withColumn("f_score", f_udf("frequency")) \
        .withColumn("m_score", m_udf("monetary")) \
        .withColumn("rfm_total", col("r_score") + col("f_score") + col("m_score"))

    # Step 3: 关联商品策略（示例：高价值客户推高端品，普通客户推优惠）
    rfm_strategy = rfm_scored.withColumn("product_strategy", when(col("rfm_total") >= 12, "推荐高端SKU")
                                         .when(col("rfm_total") >= 8, "主推性价比品")
                                         .otherwise("清库存/优惠"))

    # Step 4: 关联回原数据，让看板展示“分层+策略”
    return df.join(rfm_strategy, ["platform", "channel"], "left")


# 执行 RFM 分层
df_with_rfm = add_rfm_layers(df)

# 保存结果（含 RFM 分层）
output_dir = "metrics_data_with_rfm"
output_file = f"{output_dir}/mock_with_rfm.csv"

if os.path.exists(output_dir):
    shutil.rmtree(output_dir)

df_with_rfm.coalesce(1) \
    .write \
    .format("csv") \
    .option("header", "true") \
    .option("dateFormat", "yyyy-MM-dd") \
    .mode("overwrite") \
    .save(output_dir)

# 重命名文件
part_files = glob.glob(f"{output_dir}/part-*.csv")
if part_files:
    os.rename(part_files[0], output_file)
    for f in glob.glob(f"{output_dir}/.*") + glob.glob(f"{output_dir}/part-*"):
        if os.path.isfile(f):
            os.remove(f)

print(f"含 RFM 分层的数据已保存到 {output_file}")
df_with_rfm.show(10, False)

spark.stop()