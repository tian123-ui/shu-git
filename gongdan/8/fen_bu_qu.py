# 营销工具客服专属优惠看板代码
# 工单编号：大数据-电商数仓-08-商品主题营销工具客服专属优惠看板
# 功能：实现客服专属优惠看板的全部指标计算，包括不分层和分层(RFM)两种方式
# 技术栈：Spark 3.2+、PySpark、SparkSQL

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
import datetime


def create_spark_session():
    """创建SparkSession"""
    return SparkSession.builder \
        .appName("CustomerServicePromotionDashboard") \
        .config("spark.sql.shuffle.partitions", "20") \
        .getOrCreate()


def load_data(spark, data_path):
    """加载所需数据"""
    # 工单编号：大数据-电商数仓-08-商品主题营销工具客服专属优惠看板
    # 加载活动表
    activity_df = spark.read.csv(f"{data_path}/ods_customer_service_promo_activity.csv",
                                 header=True, inferSchema=True)

    # 加载活动-商品关联表
    activity_item_df = spark.read.csv(f"{data_path}/ods_customer_service_promo_activity_item.csv",
                                      header=True, inferSchema=True)

    # 加载优惠发送表
    send_df = spark.read.csv(f"{data_path}/ods_customer_service_promo_send.csv",
                             header=True, inferSchema=True)

    # 加载优惠核销表
    use_df = spark.read.csv(f"{data_path}/ods_customer_service_promo_use.csv",
                            header=True, inferSchema=True)

    # 加载客服表
    cs_df = spark.read.csv(f"{data_path}/dim_customer_service.csv",
                           header=True, inferSchema=True)

    # 加载商品表
    product_df = spark.read.csv(f"{data_path}/dim_product.csv",
                                header=True, inferSchema=True)

    return activity_df, activity_item_df, send_df, use_df, cs_df, product_df


def calculate_overview_non_layered(spark, send_df, use_df):
    """
    不分层方式计算概览指标
    包括：发送次数、支付次数、核销率、总优惠金额、总支付金额
    """
    # 工单编号：大数据-电商数仓-08-商品主题营销工具客服专属优惠看板

    # 转换时间格式
    send_df = send_df.withColumn("send_time", to_timestamp("send_time"))
    use_df = use_df.withColumn("use_time", to_timestamp("use_time"))

    # 计算发送总次数
    total_send = send_df.count()

    # 计算支付总次数
    total_pay = use_df.count()

    # 计算核销率
    conversion_rate = total_pay / total_send if total_send > 0 else 0

    # 计算总优惠金额
    total_promo_amount = send_df.agg(sum("promotion_amount")).collect()[0][0] or 0

    # 计算总支付金额
    total_pay_amount = use_df.agg(sum("pay_amount")).collect()[0][0] or 0

    # 创建结果DataFrame
    overview_df = spark.createDataFrame([(
        "all", "all", total_send, total_pay, conversion_rate,
        total_promo_amount, total_pay_amount, datetime.datetime.now()
    )], ["period_type", "period_value", "total_send_count", "total_pay_count",
         "conversion_rate", "total_promo_amount", "total_pay_amount", "etl_time"])

    # 写入ADS层表
    overview_df.write.mode("overwrite").saveAsTable("ads_customer_service_promo_overview_non_layered")

    return overview_df


def calculate_time_trend_non_layered(spark, send_df, use_df):
    """不分层方式计算时间趋势指标（日/7天/30天）"""
    # 工单编号：大数据-电商数仓-08-商品主题营销工具客服专属优惠看板

    # 转换时间格式并提取日期
    send_df = send_df.withColumn("send_time", to_timestamp("send_time")) \
        .withColumn("send_date", to_date("send_time"))

    use_df = use_df.withColumn("use_time", to_timestamp("use_time")) \
        .withColumn("use_date", to_date("use_time"))

    # 按日统计发送量
    daily_send = send_df.groupBy("send_date") \
        .agg(count("send_id").alias("daily_send_count")) \
        .withColumnRenamed("send_date", "stat_date")

    # 按日统计核销量
    daily_use = use_df.groupBy("use_date") \
        .agg(count("use_id").alias("daily_use_count"),
             sum("pay_amount").alias("daily_pay_amount")) \
        .withColumnRenamed("use_date", "stat_date")

    # 合并发送和核销数据
    daily_trend = daily_send.join(daily_use, on="stat_date", how="full_outer") \
        .fillna(0) \
        .withColumn("conversion_rate",
                    col("daily_use_count") / col("daily_send_count")) \
        .withColumn("period_type", lit("day"))

    # 计算7天滚动趋势
    window_7d = Window.orderBy("stat_date").rowsBetween(-6, 0)
    weekly_trend = daily_trend.select(
        "stat_date",
        sum("daily_send_count").over(window_7d).alias("weekly_send_count"),
        sum("daily_use_count").over(window_7d).alias("weekly_use_count"),
        sum("daily_pay_amount").over(window_7d).alias("weekly_pay_amount")
    ).withColumn("conversion_rate",
                 col("weekly_use_count") / col("weekly_send_count")) \
        .withColumn("period_type", lit("7days")) \
        .filter(col("stat_date") >= date_add(first("stat_date").over(Window.orderBy("stat_date")), 6))

    # 计算30天滚动趋势
    window_30d = Window.orderBy("stat_date").rowsBetween(-29, 0)
    monthly_trend = daily_trend.select(
        "stat_date",
        sum("daily_send_count").over(window_30d).alias("monthly_send_count"),
        sum("daily_use_count").over(window_30d).alias("monthly_use_count"),
        sum("daily_pay_amount").over(window_30d).alias("monthly_pay_amount")
    ).withColumn("conversion_rate",
                 col("monthly_use_count") / col("monthly_send_count")) \
        .withColumn("period_type", lit("30days")) \
        .filter(col("stat_date") >= date_add(first("stat_date").over(Window.orderBy("stat_date")), 29))

    # 合并所有时间趋势数据并写入ADS层
    time_trend_df = daily_trend.select(
        col("stat_date"),
        col("period_type"),
        col("daily_send_count").alias("send_count"),
        col("daily_use_count").alias("use_count"),
        col("daily_pay_amount").alias("pay_amount"),
        col("conversion_rate"),
        current_timestamp().alias("etl_time")
    ).unionByName(weekly_trend.select(
        col("stat_date"),
        col("period_type"),
        col("weekly_send_count").alias("send_count"),
        col("weekly_use_count").alias("use_count"),
        col("weekly_pay_amount").alias("pay_amount"),
        col("conversion_rate"),
        current_timestamp().alias("etl_time")
    )).unionByName(monthly_trend.select(
        col("stat_date"),
        col("period_type"),
        col("monthly_send_count").alias("send_count"),
        col("monthly_use_count").alias("use_count"),
        col("monthly_pay_amount").alias("pay_amount"),
        col("conversion_rate"),
        current_timestamp().alias("etl_time")
    ))

    # 写入ADS层表
    time_trend_df.write.mode("overwrite").saveAsTable("ads_customer_service_promo_time_trend_non_layered")

    return time_trend_df


def calculate_activity_effect_non_layered(spark, activity_df, send_df, use_df):
    """不分层方式计算各活动效果指标"""
    # 工单编号：大数据-电商数仓-08-商品主题营销工具客服专属优惠看板

    # 按活动统计发送次数
    activity_send = send_df.groupBy("activity_id") \
        .agg(count("send_id").alias("send_count"),
             sum("promotion_amount").alias("total_promo_amount"))

    # 按活动统计核销次数
    activity_use = use_df.join(send_df.select("send_id", "activity_id"), on="send_id", how="left") \
        .groupBy("activity_id") \
        .agg(count("use_id").alias("use_count"),
             sum("pay_amount").alias("total_pay_amount"))

    # 合并活动数据并计算转化率
    activity_effect = activity_df.join(activity_send, on="activity_id", how="left") \
        .join(activity_use, on="activity_id", how="left") \
        .fillna(0) \
        .withColumn("conversion_rate",
                    col("use_count") / col("send_count")) \
        .withColumn("etl_time", current_timestamp())

    # 添加活动排名
    window_rank = Window.orderBy(col("conversion_rate").desc())
    activity_effect = activity_effect.withColumn("rank", row_number().over(window_rank))

    # 写入ADS层表
    activity_effect.write.mode("overwrite").saveAsTable("ads_customer_service_promo_activity_effect_non_layered")

    return activity_effect


def calculate_customer_service_performance_non_layered(spark, cs_df, send_df, use_df):
    """不分层方式计算各客服绩效指标"""
    # 工单编号：大数据-电商数仓-08-商品主题营销工具客服专属优惠看板

    # 按客服统计发送次数
    cs_send = send_df.groupBy("customer_service_id") \
        .agg(count("send_id").alias("send_count"),
             sum("promotion_amount").alias("total_promo_amount"),
             avg("promotion_amount").alias("avg_promo_amount"))

    # 按客服统计核销次数
    cs_use = use_df.join(send_df.select("send_id", "customer_service_id"), on="send_id", how="left") \
        .groupBy("customer_service_id") \
        .agg(count("use_id").alias("use_count"),
             sum("pay_amount").alias("total_pay_amount"))

    # 合并客服数据并计算转化率
    cs_performance = cs_df.join(cs_send, on="customer_service_id", how="left") \
        .join(cs_use, on="customer_service_id", how="left") \
        .fillna(0) \
        .withColumn("conversion_rate",
                    col("use_count") / col("send_count")) \
        .withColumn("etl_time", current_timestamp())

    # 写入ADS层表
    cs_performance.write.mode("overwrite").saveAsTable("ads_customer_service_performance_non_layered")

    return cs_performance


def calculate_rfm_layer(spark, send_df, use_df):
    """计算RFM分层"""
    # 工单编号：大数据-电商数仓-08-商品主题营销工具客服专属优惠看板

    # 转换时间格式
    send_df = send_df.withColumn("send_time", to_timestamp("send_time"))
    use_df = use_df.withColumn("use_time", to_timestamp("use_time"))

    # 获取当前日期
    current_date = max(use_df.select(max("use_time")).collect()[0][0],
                       send_df.select(max("send_time")).collect()[0][0])

    # 合并发送和核销数据
    user_data = send_df.join(use_df, on="send_id", how="left") \
        .select("consumer_id", "send_time", "use_time", "pay_amount")

    # 计算RFM指标
    rfm_df = user_data.groupBy("consumer_id").agg(
        datediff(lit(current_date), max("use_time")).alias("R"),  # 最近消费时间
        count("send_time").alias("F"),  # 消费频率
        sum("pay_amount").alias("M")  # 消费金额
    ).fillna({
        "R": 999,  # 没有核销记录的用户，R值设为999
        "F": 0,
        "M": 0
    })

    # 计算RFM得分
    r_score = when(col("R") <= 7, 5) \
        .when(col("R") <= 30, 4) \
        .when(col("R") <= 90, 3) \
        .when(col("R") <= 180, 2) \
        .otherwise(1)

    f_score = when(col("F") >= 10, 5) \
        .when(col("F") >= 5, 4) \
        .when(col("F") >= 3, 3) \
        .when(col("F") >= 1, 2) \
        .otherwise(1)

    m_score = when(col("M") >= 10000, 5) \
        .when(col("M") >= 5000, 4) \
        .when(col("M") >= 1000, 3) \
        .when(col("M") >= 100, 2) \
        .otherwise(1)

    rfm_score = rfm_df.withColumn("R_score", r_score) \
        .withColumn("F_score", f_score) \
        .withColumn("M_score", m_score) \
        .withColumn("RFM_total", col("R_score") + col("F_score") + col("M_score"))

    # 客户分层
    user_level = when(col("RFM_total") > 12, "高价值用户") \
        .when(col("RFM_total") > 8, "潜力用户") \
        .when(col("RFM_total") > 5, "一般用户") \
        .otherwise("流失用户")

    rfm_layer = rfm_score.withColumn("user_level", user_level)

    # 写入RFM分层结果表
    rfm_layer.write.mode("overwrite").saveAsTable("dws_customer_rfm_layer")

    return rfm_layer


def calculate_overview_layered(spark, send_df, use_df, rfm_layer):
    """基于RFM分层计算概览指标"""
    # 工单编号：大数据-电商数仓-08-商品主题营销工具客服专属优惠看板

    # 关联用户分层信息
    send_with_layer = send_df.join(rfm_layer.select("consumer_id", "user_level"),
                                   on="consumer_id", how="left")

    use_with_layer = use_df.join(send_df.select("send_id", "consumer_id"),
                                 on="send_id", how="left") \
        .join(rfm_layer.select("consumer_id", "user_level"),
              on="consumer_id", how="left")

    # 按用户分层统计发送次数
    layer_send = send_with_layer.groupBy("user_level") \
        .agg(count("send_id").alias("total_send_count"),
             sum("promotion_amount").alias("total_promo_amount"))

    # 按用户分层统计核销次数
    layer_use = use_with_layer.groupBy("user_level") \
        .agg(count("use_id").alias("total_use_count"),
             sum("pay_amount").alias("total_pay_amount"))

    # 合并数据并计算转化率
    overview_layered = layer_send.join(layer_use, on="user_level", how="full_outer") \
        .fillna(0) \
        .withColumn("conversion_rate",
                    col("total_use_count") / col("total_send_count")) \
        .withColumn("period_type", lit("all")) \
        .withColumn("period_value", lit("all")) \
        .withColumn("etl_time", current_timestamp())

    # 写入ADS层表
    overview_layered.write.mode("overwrite").saveAsTable("ads_customer_service_promo_overview_layered")

    return overview_layered


def calculate_activity_effect_layered(spark, activity_df, send_df, use_df, rfm_layer):
    """基于RFM分层计算各活动效果指标"""
    # 工单编号：大数据-电商数仓-08-商品主题营销工具客服专属优惠看板

    # 关联用户分层信息
    send_with_layer = send_df.join(rfm_layer.select("consumer_id", "user_level"),
                                   on="consumer_id", how="left")

    # 按活动和用户分层统计发送次数
    activity_layer_send = send_with_layer.groupBy("activity_id", "user_level") \
        .agg(count("send_id").alias("send_count"),
             sum("promotion_amount").alias("total_promo_amount"))

    # 按活动和用户分层统计核销次数
    activity_layer_use = use_df.join(send_with_layer.select("send_id", "activity_id", "user_level"),
                                     on="send_id", how="left") \
        .groupBy("activity_id", "user_level") \
        .agg(count("use_id").alias("use_count"),
             sum("pay_amount").alias("total_pay_amount"))

    # 合并数据并计算转化率
    activity_effect_layered = activity_df.join(activity_layer_send, on="activity_id", how="left") \
        .join(activity_layer_use, on=["activity_id", "user_level"], how="left") \
        .fillna(0) \
        .withColumn("conversion_rate",
                    col("use_count") / col("send_count")) \
        .withColumn("etl_time", current_timestamp())

    # 写入ADS层表
    activity_effect_layered.write.mode("overwrite").saveAsTable("ads_customer_service_promo_activity_effect_layered")

    return activity_effect_layered


def calculate_customer_service_performance_layered(spark, cs_df, send_df, use_df, rfm_layer):
    """基于RFM分层计算各客服绩效指标"""
    # 工单编号：大数据-电商数仓-08-商品主题营销工具客服专属优惠看板

    # 关联用户分层信息
    send_with_layer = send_df.join(rfm_layer.select("consumer_id", "user_level"),
                                   on="consumer_id", how="left")

    # 按客服和用户分层统计发送次数
    cs_layer_send = send_with_layer.groupBy("customer_service_id", "user_level") \
        .agg(count("send_id").alias("send_count"),
             sum("promotion_amount").alias("total_promo_amount"),
             avg("promotion_amount").alias("avg_promo_amount"))

    # 按客服和用户分层统计核销次数
    cs_layer_use = use_df.join(send_with_layer.select("send_id", "customer_service_id", "user_level"),
                               on="send_id", how="left") \
        .groupBy("customer_service_id", "user_level") \
        .agg(count("use_id").alias("use_count"),
             sum("pay_amount").alias("total_pay_amount"))

    # 合并数据并计算转化率
    cs_performance_layered = cs_df.join(cs_layer_send, on="customer_service_id", how="left") \
        .join(cs_layer_use, on=["customer_service_id", "user_level"], how="left") \
        .fillna(0) \
        .withColumn("conversion_rate",
                    col("use_count") / col("send_count")) \
        .withColumn("etl_time", current_timestamp())

    # 写入ADS层表
    cs_performance_layered.write.mode("overwrite").saveAsTable("ads_customer_service_performance_layered")

    return cs_performance_layered


def main():
    """主函数：执行所有指标计算"""
    # 工单编号：大数据-电商数仓-08-商品主题营销工具客服专属优惠看板

    # 创建Spark会话
    spark = create_spark_session()

    # 数据路径（实际使用时替换为真实路径）
    data_path = "data"

    # 加载数据
    activity_df, activity_item_df, send_df, use_df, cs_df, product_df = load_data(spark, data_path)

    # 不分层方式计算指标
    print("开始计算不分层指标...")
    overview_non_layered = calculate_overview_non_layered(spark, send_df, use_df)
    time_trend_non_layered = calculate_time_trend_non_layered(spark, send_df, use_df)
    activity_effect_non_layered = calculate_activity_effect_non_layered(spark, activity_df, send_df, use_df)
    cs_performance_non_layered = calculate_customer_service_performance_non_layered(spark, cs_df, send_df, use_df)

    # 计算RFM分层
    print("开始计算RFM分层...")
    rfm_layer = calculate_rfm_layer(spark, send_df, use_df)

    # 分层方式计算指标
    print("开始计算分层指标...")
    overview_layered = calculate_overview_layered(spark, send_df, use_df, rfm_layer)
    activity_effect_layered = calculate_activity_effect_layered(spark, activity_df, send_df, use_df, rfm_layer)
    cs_performance_layered = calculate_customer_service_performance_layered(spark, cs_df, send_df, use_df, rfm_layer)

    # 打印部分结果用于验证
    print("不分层概览指标：")
    overview_non_layered.show()

    print("RFM分层概览指标：")
    overview_layered.show()

    # 关闭Spark会话
    spark.stop()
    print("所有指标计算完成！")


if __name__ == "__main__":
    main()


