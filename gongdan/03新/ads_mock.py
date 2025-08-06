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
    .appName("商品360看板ADS层生成") \
    .getOrCreate()

# ---------------------- 读取数据 ----------------------
# 读取DWS层数据
dws_sales_summary = spark.read.csv("mock_data/dws_sales_summary", header=True, inferSchema=True)
dws_price_analysis_summary = spark.read.csv("mock_data/dws_price_analysis_summary", header=True, inferSchema=True)
dws_rfm_summary = spark.read.csv("mock_data/dws_rfm_summary", header=True, inferSchema=True)
dws_content_summary = spark.read.csv("mock_data/dws_content_summary", header=True, inferSchema=True)

# 读取DWD层数据（用于补充部分明细指标）
dwd_title_keyword_detail = spark.read.csv("mock_data/dwd_title_keyword_detail", header=True, inferSchema=True)
dwd_sku_sales_detail = spark.read.csv("mock_data/dwd_sku_sales_detail", header=True, inferSchema=True)

# 读取维度表
dim_product = spark.read.csv("mock_data/dim_product_sku", header=True, inferSchema=True)
dim_user = spark.read.csv("mock_data/dim_user", header=True, inferSchema=True)

# ---------------------- 1. 生成ads_product_core_overview（商品核心概览表） ----------------------
# 计算环比增长窗口
growth_window = Window.partitionBy("商品ID").orderBy("日期")

# 关联流量数据（简化处理，实际应关联流量汇总表）
traffic_summary = dws_sales_summary.groupBy("商品ID", "日期") \
    .agg(
    sum("总销量").alias("订单量"),
    countDistinct("SKU ID").alias("活跃SKU数")
) \
    .withColumn("访客数", round(col("订单量") * rand() * 5 + col("订单量") * 2))  # 模拟访客数

ads_product_core_overview = dws_sales_summary \
    .join(traffic_summary, ["商品ID", "日期"], "left") \
    .join(dim_product.select("商品ID", "商品名称"), ["商品ID"], "left") \
    .withColumn("昨日营收", lag(col("总营收"), 1).over(growth_window)) \
    .withColumn("昨日销量", lag(col("总销量"), 1).over(growth_window)) \
    .withColumn("昨日访客数", lag(col("访客数"), 1).over(growth_window)) \
    .withColumn("营收环比增幅",
                when(col("昨日营收").isNull(), 0)
                .when(col("昨日营收") == 0, 0)
                .otherwise(round((col("总营收") - col("昨日营收")) / col("昨日营收"), 4))) \
    .withColumn("销量环比增幅",
                when(col("昨日销量").isNull(), 0)
                .when(col("昨日销量") == 0, 0)
                .otherwise(round((col("总销量") - col("昨日销量")) / col("昨日销量"), 4))) \
    .withColumn("访客环比增幅",
                when(col("昨日访客数").isNull(), 0)
                .when(col("昨日访客数") == 0, 0)
                .otherwise(round((col("访客数") - col("昨日访客数")) / col("昨日访客数"), 4))) \
    .withColumn("新客折扣活动销量增幅",
                when(col("日期").between("2025-01-10", "2025-01-20"),
                     round(col("新客折扣活动销量") / lag(col("总销量"), 7).over(growth_window) - 1, 4))
                .otherwise(None)) \
    .select(
    "商品ID",
    "商品名称",
    "日期",
    "总营收",
    "总销量",
    "访客数",
    "活跃SKU数",
    "营收环比增幅",
    "销量环比增幅",
    "访客环比增幅",
    "新客折扣活动销量增幅",
    "所属类目"
)

ads_product_core_overview.write.mode("overwrite").option("header", "true").csv("mock_data/ads_product_core_overview")
print("ads_product_core_overview生成完成")

# ---------------------- 2. 生成ads_sku_hot_sale（SKU热销分析表） ----------------------
# SKU排名窗口
sku_sales_rank = Window.partitionBy("商品ID", "日期").orderBy(col("销量占比").desc())

# 计算SKU销量占比
sku_sales_ratio = dwd_sku_sales_detail \
    .groupBy("商品ID", "SKU ID", "日期") \
    .agg(sum("销售数量").alias("sku销量")) \
    .withColumn("总销量", sum("sku销量").over(Window.partitionBy("商品ID", "日期"))) \
    .withColumn("销量占比", round(col("sku销量") / col("总销量"), 4))

ads_sku_hot_sale = sku_sales_ratio \
    .join(dim_product.select("商品ID", "SKU ID", "SKU属性", "商品名称"), ["商品ID", "SKU ID"], "left") \
    .join(dwd_sku_sales_detail.select("商品ID", "SKU ID", "库存余量").distinct(), ["商品ID", "SKU ID"], "left") \
    .withColumn("热销程度排名", row_number().over(sku_sales_rank)) \
    .withColumn("库存预警",
                when(col("库存余量") < 50, "紧急补货")
                .when(col("库存余量") < 100, "库存偏低")
                .otherwise("库存正常")) \
    .withColumn("推广建议",
                when(col("热销程度排名") <= 3, "重点推广")
                .when(col("销量占比") < 0.05, "考虑下架")
                .otherwise("正常销售")) \
    .select(
    "商品ID",
    "商品名称",
    "SKU ID",
    "SKU属性",
    "日期",
    "sku销量",
    "销量占比",
    "热销程度排名",
    "库存余量",
    "库存预警",
    "推广建议"
)

ads_sku_hot_sale.write.mode("overwrite").option("header", "true").csv("mock_data/ads_sku_hot_sale")
print("ads_sku_hot_sale生成完成")

# ---------------------- 3. 生成ads_price_strategy（价格策略表） ----------------------
# 价格排名窗口
price_rank_window = Window.partitionBy("所属类目", "价格区间", "日期").orderBy(col("当日均价"))

ads_price_strategy = dws_price_analysis_summary \
    .withColumn("本店价格水位",
                when(col("当日均价") > col("同类目同价格带均价"), "高于均值")
                .when(col("当日均价") < col("同类目同价格带均价"), "低于均值")
                .otherwise("等于均值")) \
    .withColumn("同类目价格排行榜", rank().over(price_rank_window)) \
    .withColumn("定价建议",
                when(col("价格力星级") == 5, "维持价格")
                .when(col("价格力星级") <= 2, "考虑降价")
                .when(col("价格趋势") == "上涨", "可适度提价")
                .otherwise("保持稳定")) \
    .select(
    "商品ID",
    "日期",
    "当日均价",
    "价格区间",
    "本店价格水位",
    "价格力星级",
    "同类目同价格带均价",
    "同类目价格排行榜",
    "定价建议",
    "所属类目"
)

ads_price_strategy.write.mode("overwrite").option("header", "true").csv("mock_data/ads_price_strategy")
print("ads_price_strategy生成完成")

# ---------------------- 4. 生成ads_title_optimization（标题优化表） ----------------------
# 词根效果排名窗口
keyword_effect_rank = Window.partitionBy("商品ID").orderBy(col("平均词根转化率").desc())

# 提取当前标题词根（模拟，实际应从商品表获取）
current_title_keywords = dim_product \
    .withColumn("当前标题词根", split(col("商品名称"), " ").alias("当前标题词根")) \
    .select("商品ID", explode(col("当前标题词根")).alias("当前标题词根"))

# 先关联商品维度表获取所属类目，再计算优质词根推荐
keyword_with_category = dwd_title_keyword_detail \
    .join(dim_product.select("商品ID", "所属类目"), ["商品ID"], "left")  # 从商品表获取类目

# 优质词根推荐（取同类目高转化词根）
recommended_keywords = keyword_with_category \
    .groupBy("所属类目", "标题词根") \
    .agg(
    avg("词根转化率").alias("平均转化率"),
    sum("引流人数（UV）").alias("总引流人数")
) \
    .filter((col("平均转化率") > 0.1) & (col("总引流人数") > 100)) \
    .select(
    col("所属类目"),
    col("标题词根").alias("推荐词根"),
    col("平均转化率").alias("推荐词根转化率")
)

# 为所有涉及的表添加别名，彻底解决字段歧义
ads_title_optimization = dwd_title_keyword_detail \
    .groupBy("商品ID", "标题词根") \
    .agg(
    sum("搜索次数").alias("总搜索次数"),
    sum("引流人数（UV）").alias("总引流人数"),
    avg("词根转化率").alias("平均词根转化率")
) \
    .withColumn("词根引流效果排名", row_number().over(keyword_effect_rank)) \
    .alias("left_df") \
    .join(current_title_keywords.alias("ctk"),
          [col("left_df.商品ID") == col("ctk.商品ID"),
           col("left_df.标题词根") == col("ctk.当前标题词根")],
          "left") \
.join(dim_product.select("商品ID", "所属类目").alias("dp"),
      col("left_df.商品ID") == col("dp.商品ID"),
      "left") \
    .join(recommended_keywords.alias("rk"),
          col("dp.所属类目") == col("rk.所属类目"),
          "left") \
    .filter(col("ctk.当前标题词根").isNotNull() | col("rk.推荐词根").isNotNull()) \
    .withColumn("修改后标题同步状态",
                when(col("rk.推荐词根").isNotNull() & col("ctk.当前标题词根").isNull(), "待同步")
                .when(col("rk.推荐词根").isNotNull() & col("ctk.当前标题词根").isNotNull(), "已同步")
                .otherwise("无建议")) \
    .select(
    col("left_df.商品ID").alias("商品ID"),
    col("ctk.当前标题词根").alias("当前标题词根"),
    "词根引流效果排名",
    "总搜索次数",
    "总引流人数",
    "平均词根转化率",
    col("rk.推荐词根").alias("推荐词根"),
    col("rk.推荐词根转化率").alias("推荐词根转化率"),
    "修改后标题同步状态"
)

ads_title_optimization.write.mode("overwrite").option("header", "true").csv("mock_data/ads_title_optimization")
print("ads_title_optimization生成完成")

# ---------------------- 5. 生成ads_rfm_strategy（RFM用户分层策略表） ----------------------
# 用户分层逻辑
ads_rfm_strategy = dws_rfm_summary \
    .join(dim_user.select("用户ID", "会员等级"), ["用户ID"], "left") \
    .join(dim_product.select("商品ID", "商品名称", "所属类目"), ["商品ID"], "left") \
    .withColumn("用户分层",
                when(col("rfm_total") >= 12, "高价值用户")
                .when(col("rfm_total") >= 8, "潜力用户")
                .when(col("rfm_total") >= 5, "一般用户")
                .otherwise("流失用户")) \
    .withColumn("商品策略",
                when(col("用户分层") == "高价值用户", "推荐高端SKU")
                .when(col("用户分层") == "潜力用户", "推荐性价比商品")
                .when(col("用户分层") == "流失用户", "推荐清库存商品")
                .otherwise("常规推荐")) \
    .withColumn("直通车投放人群建议",
                when(col("用户分层") == "高价值用户", "高溢价投放")
                .when(col("用户分层") == "潜力用户", "中等溢价投放")
                .otherwise("低溢价或不投放")) \
    .select(
    "用户ID",
    "会员等级",
    "商品ID",
    "商品名称",
    "用户分层",
    "r_score",
    "f_score",
    "m_score",
    "rfm_total",
    "商品策略",
    "直通车投放人群建议",
    "所属类目"
)

ads_rfm_strategy.write.mode("overwrite").option("header", "true").csv("mock_data/ads_rfm_strategy")
print("ads_rfm_strategy生成完成")

spark.stop()
