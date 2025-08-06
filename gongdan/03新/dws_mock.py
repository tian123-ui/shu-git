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
    .appName("商品360看板DWS层生成") \
    .getOrCreate()

# ---------------------- 读取数据 ----------------------
# 读取DWD层数据
dwd_sku_sales_detail = spark.read.csv("mock_data/dwd_sku_sales_detail", header=True, inferSchema=True)
dwd_traffic_channel_detail = spark.read.csv("mock_data/dwd_traffic_channel_detail", header=True, inferSchema=True)
dwd_title_keyword_detail = spark.read.csv("mock_data/dwd_title_keyword_detail", header=True, inferSchema=True)
dwd_content_effect_detail = spark.read.csv("mock_data/dwd_content_effect_detail", header=True, inferSchema=True)
dwd_rfm_behavior_detail = spark.read.csv("mock_data/dwd_rfm_behavior_detail", header=True, inferSchema=True)

# 读取维度表
dim_user = spark.read.csv("mock_data/dim_user", header=True, inferSchema=True)
dim_product = spark.read.csv("mock_data/dim_product_sku", header=True, inferSchema=True).alias("p")
dim_price = spark.read.csv("mock_data/dim_price_band", header=True, inferSchema=True).alias("pr")

# 读取原始销售数据用于获取用户ID关联（因为DWD层可能已去除用户ID）
ods_sales = spark.read.csv("mock_data/ods_sales_original", header=True, inferSchema=True)

# ---------------------- 1. 生成dws_sales_summary（销售汇总表） ----------------------
# 热销SKU排名窗口
sku_rank_window = Window.partitionBy("商品ID", "日期").orderBy(col("销量占比").desc())

# 从原始销售数据获取用户ID与商品的关联关系
user_product_mapping = ods_sales.select(
    col("商品ID"),
    col("SKU ID"),
    col("购买用户ID").alias("用户ID")
).distinct()

dws_sales_summary = dwd_sku_sales_detail \
.join(user_product_mapping, ["商品ID", "SKU ID"], "left") \
.join(dim_user, ["用户ID"], "left") \
.withColumn("用户标签", when(col("用户标签").isNull(), "未知").otherwise(col("用户标签"))) \
    .withColumn("新客订单", when(col("用户标签") == "新客", col("销售数量")).otherwise(0)) \
    .withColumn("老客订单", when(col("用户标签") == "老客", col("销售数量")).otherwise(0)) \
    .groupBy("商品ID", "日期", "所属类目") \
    .agg(
    sum("销售金额").alias("总营收"),
    sum("销售数量").alias("总销量"),
    sum("新客订单").alias("新客总销量"),
    sum("老客订单").alias("老客总销量"),
    # 模拟运营节点关联销量
    sum(when(col("日期").between("2025-01-10", "2025-01-20"), col("销售数量")).otherwise(0)).alias("新客折扣活动销量"),
    sum(when(col("日期").between("2025-02-01", "2025-02-15"), col("销售数量")).otherwise(0)).alias("分期免息活动销量")
) \
    .withColumn("新客订单占比",
                when(col("总销量") == 0, 0)
                .otherwise(round(col("新客总销量") / col("总销量"), 4))) \
    .withColumn("老客订单占比",
                when(col("总销量") == 0, 0)
                .otherwise(round(col("老客总销量") / col("总销量"), 4))) \
.join(
    dwd_sku_sales_detail.groupBy("商品ID", "日期", "SKU ID")
    .agg(sum("销售数量").alias("sku销量"))
    .withColumn("总销量", sum("sku销量").over(Window.partitionBy("商品ID", "日期")))
    .withColumn("销量占比",
                when(col("总销量") == 0, 0)
                .otherwise(col("sku销量") / col("总销量")))
    .withColumn("热销SKU排名", row_number().over(sku_rank_window))
    .filter(col("热销SKU排名") <= 3)  # 取TOP3热销SKU
    .select("商品ID", "日期", "SKU ID", "热销SKU排名"),
    ["商品ID", "日期"],
    "left"
) \
    .select(
    "商品ID",
    "日期",
    "总营收",
    "总销量",
    "热销SKU排名",
    "SKU ID",
    "新客订单占比",
    "老客订单占比",
    "新客折扣活动销量",
    "分期免息活动销量",
    "所属类目"
)

dws_sales_summary.write.mode("overwrite").option("header", "true").csv("mock_data/dws_sales_summary")
print("dws_sales_summary生成完成")

# ---------------------- 2. 生成dws_price_analysis_summary（价格分析汇总表） ----------------------
# 价格趋势计算窗口（获取前一天价格）
price_lag_window = Window.partitionBy("商品ID").orderBy("日期")

dws_price_analysis_summary = dwd_sku_sales_detail \
    .groupBy("商品ID", "日期", "价格区间", "所属类目") \
    .agg(
    avg(col("销售金额") / col("销售数量")).alias("当日均价"),  # 修复表达式格式
    count("SKU ID").alias("sku_count")
) \
    .withColumn("前日均价", lag(col("当日均价"), 1).over(price_lag_window)) \
    .withColumn("价格趋势",
                when(col("前日均价").isNull(), "无历史数据")
                .when(col("当日均价") > col("前日均价"), "上涨")
                .when(col("当日均价") < col("前日均价"), "下跌")
                .otherwise("持平")) \
    .withColumn("价格变动幅度",
                when(col("前日均价").isNull(), 0)
                .when(col("前日均价") == 0, 0)
                .otherwise(round((col("当日均价") - col("前日均价")) / col("前日均价"), 4))) \
.join(
    dwd_sku_sales_detail.groupBy("所属类目", "价格区间", "日期")
    .agg(avg(col("销售金额") / col("销售数量")).alias("同类目同价格带均价")),
    ["所属类目", "价格区间", "日期"],
    "left"
) \
.withColumn("价格力星级",
            when(col("同类目同价格带均价").isNull(), 3)  # 处理基准价缺失情况
            .when(col("当日均价") < col("同类目同价格带均价") * 0.9, 5)
            .when(col("当日均价") < col("同类目同价格带均价"), 4)
            .when(col("当日均价") == col("同类目同价格带均价"), 3)
            .when(col("当日均价") > col("同类目同价格带均价") * 1.1, 1)
            .otherwise(2)) \
    .select(
    "商品ID",
    "日期",
    "当日均价",
    "价格趋势",
    "价格变动幅度",
    "价格区间",
    "价格力星级",
    "同类目同价格带均价",
    "所属类目"
)

dws_price_analysis_summary.write.mode("overwrite").option("header", "true").csv("mock_data/dws_price_analysis_summary")
print("dws_price_analysis_summary生成完成")

# ---------------------- 3. 生成dws_rfm_summary（RFM汇总表） ----------------------
# 计算RFM得分的窗口（按分位数划分等级）
r_score_window = Window.partitionBy("商品ID").orderBy(col("recency_days"))
f_score_window = Window.partitionBy("商品ID").orderBy(col("消费次数（frequency）"))
m_score_window = Window.partitionBy("商品ID").orderBy(col("累计消费金额（monetary）"))

dws_rfm_summary = dwd_rfm_behavior_detail \
    .withColumn("当前日期", lit(datetime.now().strftime("%Y-%m-%d"))) \
    .withColumn("recency_days",
                datediff(to_date(col("当前日期")), to_date(col("最近消费时间（recency）")))) \
.withColumn("r_percentile", percent_rank().over(r_score_window)) \
    .withColumn("r_score",
                when(col("r_percentile") <= 0.2, 5)
                .when(col("r_percentile") <= 0.4, 4)
                .when(col("r_percentile") <= 0.6, 3)
                .when(col("r_percentile") <= 0.8, 2)
                .otherwise(1)) \
    .withColumn("f_percentile", percent_rank().over(f_score_window)) \
    .withColumn("f_score",
                when(col("f_percentile") <= 0.2, 1)
                .when(col("f_percentile") <= 0.4, 2)
                .when(col("f_percentile") <= 0.6, 3)
                .when(col("f_percentile") <= 0.8, 4)
                .otherwise(5)) \
    .withColumn("m_percentile", percent_rank().over(m_score_window)) \
    .withColumn("m_score",
                when(col("m_percentile") <= 0.2, 1)
                .when(col("m_percentile") <= 0.4, 2)
                .when(col("m_percentile") <= 0.6, 3)
                .when(col("m_percentile") <= 0.8, 4)
                .otherwise(5)) \
    .withColumn("rfm_total", col("r_score") + col("f_score") + col("m_score")) \
    .select(
    "用户ID",
    "商品ID",
    "recency_days",
    "消费次数（frequency）",
    "累计消费金额（monetary）",
    "r_score",
    "f_score",
    "m_score",
    "rfm_total",
    "最近消费时间（recency）"
)

dws_rfm_summary.write.mode("overwrite").option("header", "true").csv("mock_data/dws_rfm_summary")
print("dws_rfm_summary生成完成")

# ---------------------- 4. 生成dws_content_summary（内容汇总表） ----------------------
# 内容类型对比窗口
content_type_window = Window.partitionBy("日期")

dws_content_summary = dwd_content_effect_detail \
    .groupBy("内容类型", "日期") \
    .agg(
    sum("点击量").alias("总点击量"),
    sum("曝光量").alias("总曝光量"),
    sum(when(col("TOP50排名") <= 50, 1).otherwise(0)).alias("TOP50内容数量"),
    sum(when(col("TOP50排名") <= 50, col("点击量")).otherwise(0)).alias("TOP50内容总点击量"),
    avg("转化率").alias("平均转化率")
) \
    .withColumn("类型占比",
                when(sum("总点击量").over(content_type_window) == 0, 0)
                .otherwise(round(col("总点击量") / sum("总点击量").over(content_type_window), 4))) \
    .withColumn("TOP50点击贡献",
                when(col("总点击量") == 0, 0)
                .otherwise(round(col("TOP50内容总点击量") / col("总点击量"), 4))) \
.withColumn("直播vs短视频点击比",
            when(col("内容类型") == "直播",
                 when(sum(when(col("内容类型") == "短视频", col("总点击量")).otherwise(0)).over(
                     content_type_window) == 0, 0)
                 .otherwise(col("总点击量") / sum(when(col("内容类型") == "短视频", col("总点击量")).otherwise(0)).over(
                     content_type_window))
                 )
            .when(col("内容类型") == "短视频",
                  when(
                      sum(when(col("内容类型") == "直播", col("总点击量")).otherwise(0)).over(content_type_window) == 0,
                      0)
                  .otherwise(col("总点击量") / sum(when(col("内容类型") == "直播", col("总点击量")).otherwise(0)).over(
                      content_type_window))
                  )
            .otherwise(None)) \
    .select(
    "内容类型",
    "日期",
    "总点击量",
    "总曝光量",
    "平均转化率",
    "TOP50内容数量",
    "TOP50内容总点击量",
    "TOP50点击贡献",
    "类型占比",
    "直播vs短视频点击比"
)

dws_content_summary.write.mode("overwrite").option("header", "true").csv("mock_data/dws_content_summary")
print("dws_content_summary生成完成")

spark.stop()
