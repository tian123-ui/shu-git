
#TODO启动： streamlit run "二、RFM 客户价值模型（机器学习实现）.py"


# 基于PySpark的RFM模型与商品分析看板实现
import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, max, datediff, current_date, when, desc, rank
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st
import numpy as np
from datetime import datetime

import os
# 配置PySpark环境
os.environ["PYSPARK_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"


# 设置中文显示
plt.rcParams["font.family"] = ["SimHei", "WenQuanYi Micro Hei", "Heiti TC"]
sns.set(font='SimHei', font_scale=0.8)


class RFMAnalyzer:
    """RFM模型分析器"""

    def __init__(self, spark):
        self.spark = spark
        self.transactions_df = None  # 交易数据
        self.rfm_df = None  # RFM指标数据
        self.segmented_df = None  # 客户分群数据

    def load_data_from_path(self, data_path):
        """从文件路径加载交易数据"""
        # 假设数据格式: customer_id, order_id, order_date, amount, product_id, category
        self.transactions_df = self.spark.read.csv(
            data_path,
            header=True,
            inferSchema=True,
            timestampFormat="yyyy-MM-dd"
        )
        # 确保日期列格式正确
        self.transactions_df = self.transactions_df.withColumn(
            "order_date", col("order_date").cast("date")
        )
        return self

    def load_data_from_df(self, df):
        """从已有DataFrame加载交易数据"""
        self.transactions_df = df.withColumn(
            "order_date", col("order_date").cast("date")
        )
        return self

    def calculate_rfm(self):
        """计算RFM指标"""
        if self.transactions_df is None:
            raise ValueError("请先加载交易数据（使用load_data_from_path或load_data_from_df）")

        # 按客户ID分组计算RFM值
        self.rfm_df = self.transactions_df.groupBy("customer_id").agg(
            datediff(current_date(), max("order_date")).alias("recency"),  # 最近消费时间
            count("order_id").alias("frequency"),  # 消费频率
            sum("amount").alias("monetary")  # 消费金额
        )
        return self

    def score_rfm(self):
        """为RFM指标打分(1-5分)"""
        if self.rfm_df is None:
            self.calculate_rfm()

        # 计算总客户数（用于百分比计算）
        total_customers = self.rfm_df.count()

        # 计算R分 (值越小越好，所以反转分数)
        r_window = Window.orderBy(col("recency"))
        self.rfm_df = self.rfm_df.withColumn(
            "r_quartile",
            when(rank().over(r_window) / total_customers <= 0.2, 5)
            .when(rank().over(r_window) / total_customers <= 0.4, 4)
            .when(rank().over(r_window) / total_customers <= 0.6, 3)
            .when(rank().over(r_window) / total_customers <= 0.8, 2)
            .otherwise(1)
        )

        # 计算F分
        f_window = Window.orderBy(desc("frequency"))
        self.rfm_df = self.rfm_df.withColumn(
            "f_quartile",
            when(rank().over(f_window) / total_customers <= 0.2, 5)
            .when(rank().over(f_window) / total_customers <= 0.4, 4)
            .when(rank().over(f_window) / total_customers <= 0.6, 3)
            .when(rank().over(f_window) / total_customers <= 0.8, 2)
            .otherwise(1)
        )

        # 计算M分
        m_window = Window.orderBy(desc("monetary"))
        self.rfm_df = self.rfm_df.withColumn(
            "m_quartile",
            when(rank().over(m_window) / total_customers <= 0.2, 5)
            .when(rank().over(m_window) / total_customers <= 0.4, 4)
            .when(rank().over(m_window) / total_customers <= 0.6, 3)
            .when(rank().over(m_window) / total_customers <= 0.8, 2)
            .otherwise(1)
        )

        # 计算总分
        self.rfm_df = self.rfm_df.withColumn(
            "rfm_score",
            col("r_quartile") + col("f_quartile") + col("m_quartile")
        )

        return self

    def segment_customers(self):
        """根据RFM分数进行客户分群"""
        if self.rfm_df is None:
            self.score_rfm()

        # 定义客户分群规则
        self.segmented_df = self.rfm_df.withColumn(
            "segment",
            when((col("r_quartile") >= 4) & (col("f_quartile") >= 4) & (col("m_quartile") >= 4), "高价值客户")
            .when((col("r_quartile") >= 3) & (col("f_quartile") >= 4) & (col("m_quartile") >= 3), "忠诚客户")
            .when((col("r_quartile") >= 4) & (col("f_quartile") <= 3) & (col("m_quartile") >= 3), "潜力客户")
            .when((col("r_quartile") >= 3) & (col("f_quartile") >= 2) & (col("m_quartile") >= 2), "一般客户")
            .when((col("r_quartile") <= 3) & (col("f_quartile") >= 3) & (col("m_quartile") >= 3), "流失风险客户")
            .otherwise("流失客户")
        )
        return self

    def get_segment_distribution(self):
        """获取客户分群分布"""
        if self.segmented_df is None:
            self.segment_customers()
        return self.segmented_df.groupBy("segment").count().toPandas()

    def get_rfm_stats(self):
        """获取各分群的RFM统计数据"""
        if self.segmented_df is None:
            self.segment_customers()
        return self.segmented_df.groupBy("segment").agg(
            sum("monetary").alias("总消费金额"),
            count("customer_id").alias("客户数量"),
            avg("frequency").alias("平均消费频率"),
            avg("recency").alias("平均最近消费天数")
        ).toPandas()


class ProductAnalyzer:
    """商品主题指标分析器"""

    def __init__(self, spark, transactions_df, customer_segments_df):
        self.spark = spark
        self.transactions_df = transactions_df
        self.customer_segments_df = customer_segments_df
        self.product_metrics_df = None

    def calculate_product_metrics(self):
        """计算商品核心指标"""
        # 合并交易数据和客户分群数据
        merged_df = self.transactions_df.join(
            self.customer_segments_df.select("customer_id", "segment"),
            on="customer_id",
            how="inner"
        )

        # 计算商品基本指标
        self.product_metrics_df = merged_df.groupBy("product_id", "category").agg(
            count("order_id").alias("总销量"),
            sum("amount").alias("总销售额"),
            (sum("amount") / count("order_id")).alias("平均客单价")
        )

        return self

    def get_top_products(self, n=10):
        """获取销量TOP N商品"""
        if self.product_metrics_df is None:
            self.calculate_product_metrics()
        return self.product_metrics_df.orderBy(desc("总销量")).limit(n).toPandas()

    def get_category_sales(self):
        """获取各品类销售占比"""
        if self.product_metrics_df is None:
            self.calculate_product_metrics()

        category_sales = self.product_metrics_df.groupBy("category").agg(
            sum("总销售额").alias("品类总销售额")
        )

        total_sales = category_sales.agg(sum("品类总销售额")).collect()[0][0]

        return category_sales.withColumn(
            "销售占比",
            (col("品类总销售额") / total_sales * 100).cast(DoubleType())
        ).toPandas()

    def get_segment_product_contribution(self):
        """获取各客户分群对商品销售的贡献"""
        merged_df = self.transactions_df.join(
            self.customer_segments_df.select("customer_id", "segment"),
            on="customer_id",
            how="inner"
        )

        return merged_df.groupBy("segment", "category").agg(
            sum("amount").alias("分群销售额"),
            count("order_id").alias("分群订单数")
        ).toPandas()

    def get_sales_trend(self):
        """获取销售趋势"""
        return self.transactions_df.groupBy(
            col("order_date").substr(1, 7).alias("月份")
        ).agg(
            sum("amount").alias("月度销售额"),
            count("order_id").alias("月度订单数")
        ).orderBy("月份").toPandas()


class Dashboard:
    """可视化看板"""

    def __init__(self, rfm_analyzer, product_analyzer):
        self.rfm_analyzer = rfm_analyzer
        self.product_analyzer = product_analyzer
        self.transactions_df = None  # 交易数据（用于概览指标）
        self.setup_page()

    def setup_page(self):
        """设置页面配置"""
        st.set_page_config(
            page_title="RFM客户分析与商品360看板",
            page_icon="📊",
            layout="wide"
        )
        st.title("RFM客户分析与商品360看板")

    def display_overview_metrics(self):
        """展示概览指标"""
        if self.transactions_df is None:
            st.warning("缺少交易数据，无法展示概览指标")
            return

        col1, col2, col3, col4 = st.columns(4)

        # 总客户数
        total_customers = self.rfm_analyzer.segmented_df.count()
        col1.metric("总客户数", f"{total_customers:,}")

        # 总销售额
        total_sales = self.transactions_df.agg(sum("amount")).collect()[0][0]
        col2.metric("总销售额", f"¥{total_sales:,.2f}")

        # 平均订单金额
        avg_order_value = self.transactions_df.agg(
            sum("amount") / count("order_id")
        ).collect()[0][0]
        col3.metric("平均订单金额", f"¥{avg_order_value:,.2f}")

        # 总订单数
        total_orders = self.transactions_df.count()
        col4.metric("总订单数", f"{total_orders:,}")

    def display_rfm_analysis(self):
        """展示RFM分析结果"""
        st.subheader("RFM客户分群分析")

        # 客户分群分布
        segment_dist = self.rfm_analyzer.get_segment_distribution()
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.barplot(x="segment", y="count", data=segment_dist, ax=ax)
        ax.set_title("客户分群分布")
        ax.set_xticklabels(ax.get_xticklabels(), rotation=45)
        st.pyplot(fig)

        # RFM散点图
        rfm_pd = self.rfm_analyzer.segmented_df.select(
            "recency", "frequency", "monetary", "segment"
        ).toPandas()
        fig, ax = plt.subplots(figsize=(12, 8))
        sns.scatterplot(
            x="frequency", y="monetary", hue="segment",
            size="recency", sizes=(50, 200), alpha=0.7,
            data=rfm_pd, ax=ax
        )
        ax.set_title("客户消费频率 vs 消费金额分布")
        ax.set_xlabel("消费频率")
        ax.set_ylabel("消费金额")
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        st.pyplot(fig)

        # 各分群RFM统计
        st.subheader("各客户分群RFM指标统计")
        rfm_stats = self.rfm_analyzer.get_rfm_stats()
        st.dataframe(rfm_stats.style.format({
            "总消费金额": "¥{:,.2f}",
            "平均消费频率": "{:.2f}",
            "平均最近消费天数": "{:.1f}"
        }))

    def display_product_analysis(self):
        """展示商品分析结果"""
        st.subheader("商品主题指标分析")

        # 销售趋势
        sales_trend = self.product_analyzer.get_sales_trend()
        fig, ax1 = plt.subplots(figsize=(12, 6))

        ax1.plot(sales_trend["月份"], sales_trend["月度销售额"], 'b-', label='销售额')
        ax1.set_xlabel('月份')
        ax1.set_ylabel('销售额', color='b')
        ax1.tick_params('y', colors='b')
        ax1.set_xticklabels(sales_trend["月份"], rotation=45)

        ax2 = ax1.twinx()
        ax2.plot(sales_trend["月份"], sales_trend["月度订单数"], 'g-', label='订单数')
        ax2.set_ylabel('订单数', color='g')
        ax2.tick_params('y', colors='g')

        fig.tight_layout()
        st.pyplot(fig)

        # 销量TOP10商品
        top_products = self.product_analyzer.get_top_products(10)
        fig, ax = plt.subplots(figsize=(12, 6))
        sns.barplot(x="product_id", y="总销量", data=top_products, ax=ax)
        ax.set_title("销量TOP10商品")
        ax.set_xticklabels(ax.get_xticklabels(), rotation=45)
        st.pyplot(fig)

        # 品类销售占比
        category_sales = self.product_analyzer.get_category_sales()
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.pie(
            category_sales["销售占比"],
            labels=category_sales["category"],
            autopct='%1.1f%%',
            startangle=90
        )
        ax.set_title("商品品类销售占比")
        st.pyplot(fig)

        # 客户分群对品类销售的贡献
        segment_contribution = self.product_analyzer.get_segment_product_contribution()
        fig, ax = plt.subplots(figsize=(12, 8))
        pivot_data = segment_contribution.pivot(
            index="category", columns="segment", values="分群销售额"
        )
        pivot_data.plot(kind='bar', stacked=True, ax=ax)
        ax.set_title("各客户分群对品类销售的贡献")
        ax.set_xticklabels(ax.get_xticklabels(), rotation=45)
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        st.pyplot(fig)

    def show(self):
        """展示完整看板"""
        # 概览指标
        self.display_overview_metrics()

        # RFM分析
        self.display_rfm_analysis()

        # 商品分析
        self.display_product_analysis()


def generate_sample_data(spark, num_records=10000):
    """生成样本交易数据用于测试"""
    np.random.seed(42)

    # 生成客户ID
    customer_ids = np.random.randint(1, 1000, size=num_records)

    # 生成订单日期 (过去1年内)
    end_date = datetime.now()
    start_date = datetime(end_date.year - 1, end_date.month, end_date.day)
    date_range = (end_date - start_date).days
    order_dates = [
        (start_date + pd.Timedelta(days=np.random.randint(0, date_range))).strftime("%Y-%m-%d")
        for _ in range(num_records)
    ]

    # 生成订单金额
    amounts = np.random.lognormal(mean=5, sigma=0.8, size=num_records)
    amounts = np.round(amounts, 2)

    # 生成商品ID和品类
    product_ids = np.random.randint(1, 200, size=num_records)
    categories = np.random.choice(
        ["电子产品", "服装鞋帽", "食品饮料", "家居用品", "美妆个护"],
        size=num_records
    )

    # 创建订单ID
    order_ids = np.arange(1, num_records + 1)

    # 创建DataFrame
    data = {
        "order_id": order_ids,
        "customer_id": customer_ids,
        "order_date": order_dates,
        "amount": amounts,
        "product_id": product_ids,
        "category": categories
    }

    return spark.createDataFrame(pd.DataFrame(data))


def main():
    # 初始化Spark会话
    spark = SparkSession.builder \
        .appName("RFM Analysis and Product Dashboard") \
        .master("local[*]") \
        .getOrCreate()

    try:
        # 生成样本数据 (实际应用中替换为读取真实数据)
        st.sidebar.info("使用样本数据进行分析，实际应用中可替换为真实数据源")
        transactions_df = generate_sample_data(spark, 50000)

        # 执行RFM分析（使用DataFrame加载数据，而非文件路径）
        rfm_analyzer = RFMAnalyzer(spark)
        rfm_analyzer.load_data_from_df(transactions_df).calculate_rfm().score_rfm().segment_customers()

        # 执行商品分析
        product_analyzer = ProductAnalyzer(
            spark,
            transactions_df,
            rfm_analyzer.segmented_df
        )
        product_analyzer.calculate_product_metrics()

        # 展示看板
        dashboard = Dashboard(rfm_analyzer, product_analyzer)
        dashboard.transactions_df = transactions_df  # 传递交易数据用于概览指标
        dashboard.show()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()