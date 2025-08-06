from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import os
import random
from datetime import datetime, timedelta

# 配置PySpark环境
os.environ["PYSPARK_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"

# MySQL连接配置 - 请根据实际情况修改
MYSQL_CONFIG = {
    "url": "jdbc:mysql://localhost:3306/your_database_name?useSSL=false&serverTimezone=UTC",
    "user": "your_username",
    "password": "your_password",
    "driver": "com.mysql.cj.jdbc.Driver"
}


def create_spark_session():
    """创建并返回SparkSession实例"""
    return SparkSession.builder \
        .appName("商品360看板完整实现") \
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.28") \
        .getOrCreate()


def read_source_data(spark):
    """读取所有源数据并返回数据框字典"""
    print("开始读取源数据...")

    # 读取DWS层数据
    dws_data = {
        "sales_summary": spark.read.csv("mock_data/dws_sales_summary", header=True, inferSchema=True),
        "price_analysis_summary": spark.read.csv("mock_data/dws_price_analysis_summary", header=True, inferSchema=True),
        "rfm_summary": spark.read.csv("mock_data/dws_rfm_summary", header=True, inferSchema=True),
        "content_summary": spark.read.csv("mock_data/dws_content_summary", header=True, inferSchema=True),
        "user_activity_summary": spark.read.csv("mock_data/dws_user_activity_summary", header=True, inferSchema=True),
        "marketing_effect_summary": spark.read.csv("mock_data/dws_marketing_effect_summary", header=True,
                                                   inferSchema=True),
        "comment_analysis_summary": spark.read.csv("mock_data/dws_comment_analysis_summary", header=True,
                                                   inferSchema=True)
    }

    # 读取DWD层数据
    dwd_data = {
        "title_keyword_detail": spark.read.csv("mock_data/dwd_title_keyword_detail", header=True, inferSchema=True),
        "sku_sales_detail": spark.read.csv("mock_data/dwd_sku_sales_detail", header=True, inferSchema=True),
        "user_purchase_detail": spark.read.csv("mock_data/dwd_user_purchase_detail", header=True, inferSchema=True),
        "page_view_detail": spark.read.csv("mock_data/dwd_page_view_detail", header=True, inferSchema=True),
        "cart_detail": spark.read.csv("mock_data/dwd_cart_detail", header=True, inferSchema=True),
        "order_detail": spark.read.csv("mock_data/dwd_order_detail", header=True, inferSchema=True),
        "refund_detail": spark.read.csv("mock_data/dwd_refund_detail", header=True, inferSchema=True)
    }

    # 读取维度表
    dim_data = {
        "product": spark.read.csv("mock_data/dim_product_sku", header=True, inferSchema=True),
        "user": spark.read.csv("mock_data/dim_user", header=True, inferSchema=True),
        "category": spark.read.csv("mock_data/dim_category", header=True, inferSchema=True),
        "region": spark.read.csv("mock_data/dim_region", header=True, inferSchema=True),
        "time": spark.read.csv("mock_data/dim_time", header=True, inferSchema=True)
    }

    print("源数据读取完成")
    return dws_data, dwd_data, dim_data


# ---------------------- 1. 商品核心指标计算 ----------------------

def calculate_core_indicators(dws_sales, dim_product):
    """
    计算商品核心指标（不分层方式）
    包含销售额、销量、客单价等基础指标
    """
    print("开始计算不分层的商品核心指标...")

    # 聚合计算核心指标
    core_indicators = dws_sales.groupBy("商品ID", "日期") \
        .agg(
        sum("总营收").alias("总销售额"),
        sum("总销量").alias("总销售量"),
        avg("客单价").alias("平均客单价"),
        countDistinct("订单ID").alias("订单总数"),
        sum("退款金额").alias("总退款金额"),
        sum("优惠券抵扣金额").alias("总优惠金额")
    ) \
        .withColumn("退款率", col("总退款金额") / col("总销售额")) \
        .withColumn("优惠占比", col("总优惠金额") / col("总销售额")) \
        .join(dim_product.select("商品ID", "商品名称", "所属类目"), ["商品ID"], "left")

    print("不分层的商品核心指标计算完成")
    return core_indicators


def calculate_core_indicators_by_category(dws_sales, dim_product, dim_category):
    """
    按类目分层计算商品核心指标
    在基础指标上增加类目维度的聚合和对比
    """
    print("开始按类目分层计算商品核心指标...")

    # 先计算基础指标
    base_indicators = calculate_core_indicators(dws_sales, dim_product)

    # 计算类目层级的汇总指标
    category_summary = base_indicators.groupBy("所属类目", "日期") \
        .agg(
        sum("总销售额").alias("类目总销售额"),
        sum("总销售量").alias("类目总销售量"),
        avg("平均客单价").alias("类目平均客单价")
    )

    # 窗口函数用于计算类目内排名
    category_window = Window.partitionBy("所属类目", "日期").orderBy(col("总销售额").desc())

    # 关联类目汇总数据并计算排名和占比
    category_indicators = base_indicators \
        .join(category_summary, ["所属类目", "日期"], "left") \
        .withColumn("类目内销售排名", row_number().over(category_window)) \
        .withColumn("类目销售占比", col("总销售额") / col("类目总销售额")) \
        .join(dim_category.select("类目ID", "类目名称", "父类目名称"),
              base_indicators["所属类目"] == dim_category["类目名称"], "left")

    print("按类目分层的商品核心指标计算完成")
    return category_indicators


# ---------------------- 2. 流量与转化指标计算 ----------------------

def calculate_traffic_indicators(dwd_page, dwd_cart, dwd_order):
    """
    计算流量与转化指标（不分层方式）
    包含访客数、浏览量、加购转化率等指标
    """
    print("开始计算不分层的流量与转化指标...")

    # 计算商品访问指标
    visit_indicators = dwd_page.groupBy("商品ID", "日期") \
        .agg(
        countDistinct("用户ID").alias("访客数(UV)"),
        count("页面ID").alias("浏览量(PV)"),
        avg("停留时长").alias("平均停留时长")
    ) \
        .withColumn("PV/UV", col("浏览量(PV)") / col("访客数(UV)"))

    # 计算加购指标
    cart_indicators = dwd_cart.groupBy("商品ID", "日期") \
        .agg(
        countDistinct("用户ID").alias("加购用户数"),
        sum("加购数量").alias("总加购数量")
    )

    # 计算下单指标
    order_indicators = dwd_order.groupBy("商品ID", "日期") \
        .agg(
        countDistinct("用户ID").alias("下单用户数"),
        sum("购买数量").alias("总购买数量")
    )

    # 关联所有指标并计算转化率
    traffic_indicators = visit_indicators \
        .join(cart_indicators, ["商品ID", "日期"], "left") \
        .join(order_indicators, ["商品ID", "日期"], "left") \
        .fillna(0, subset=["加购用户数", "总加购数量", "下单用户数", "总购买数量"]) \
        .withColumn("加购转化率", col("加购用户数") / col("访客数(UV)")) \
        .withColumn("下单转化率", col("下单用户数") / col("加购用户数")) \
        .withColumn("支付转化率", col("总购买数量") / col("总加购数量"))

    print("不分层的流量与转化指标计算完成")
    return traffic_indicators


def calculate_traffic_indicators_by_region(dwd_page, dwd_cart, dwd_order, dim_user, dim_region):
    """
    按地区分层计算流量与转化指标
    在基础指标上增加地区维度的分析
    """
    print("开始按地区分层计算流量与转化指标...")

    # 关联用户地区信息
    page_with_region = dwd_page \
        .join(dim_user.select("用户ID", "地区ID"), ["用户ID"], "left") \
        .join(dim_region, ["地区ID"], "left")

    cart_with_region = dwd_cart \
        .join(dim_user.select("用户ID", "地区ID"), ["用户ID"], "left") \
        .join(dim_region, ["地区ID"], "left")

    order_with_region = dwd_order \
        .join(dim_user.select("用户ID", "地区ID"), ["用户ID"], "left") \
        .join(dim_region, ["地区ID"], "left")

    # 按地区计算访问指标
    region_visit = page_with_region.groupBy("商品ID", "日期", "省份", "城市") \
        .agg(
        countDistinct("用户ID").alias("地区访客数"),
        count("页面ID").alias("地区浏览量")
    )

    # 按地区计算加购指标
    region_cart = cart_with_region.groupBy("商品ID", "日期", "省份", "城市") \
        .agg(countDistinct("用户ID").alias("地区加购用户数"))

    # 按地区计算下单指标
    region_order = order_with_region.groupBy("商品ID", "日期", "省份", "城市") \
        .agg(countDistinct("用户ID").alias("地区下单用户数"))

    # 关联地区指标并计算转化率
    region_traffic = region_visit \
        .join(region_cart, ["商品ID", "日期", "省份", "城市"], "left") \
        .join(region_order, ["商品ID", "日期", "省份", "城市"], "left") \
        .fillna(0, subset=["地区加购用户数", "地区下单用户数"]) \
        .withColumn("地区加购转化率", col("地区加购用户数") / col("地区访客数")) \
        .withColumn("地区下单转化率", col("地区下单用户数") / col("地区加购用户数"))

    # 计算全国总指标用于对比
    national_total = calculate_traffic_indicators(dwd_page, dwd_cart, dwd_order) \
        .select(
        "商品ID", "日期",
        col("加购转化率").alias("全国加购转化率"),
        col("下单转化率").alias("全国下单转化率")
    )

    # 关联全国指标，计算地区与全国的差异
    final_indicators = region_traffic \
        .join(national_total, ["商品ID", "日期"], "left") \
        .withColumn("加购转化差异率", col("地区加购转化率") - col("全国加购转化率")) \
        .withColumn("下单转化差异率", col("地区下单转化率") - col("全国下单转化率"))

    print("按地区分层的流量与转化指标计算完成")
    return final_indicators


# ---------------------- 3. 价格与促销指标计算 ----------------------

def calculate_price_promotion_indicators(dws_price, dws_marketing):
    """
    计算价格与促销指标（不分层方式）
    包含价格波动、促销力度、促销效果等指标
    """
    print("开始计算不分层的价格与促销指标...")

    # 价格指标
    price_indicators = dws_price.groupBy("商品ID", "日期") \
        .agg(
        avg("当日均价").alias("平均售价"),
        max("当日最高价").alias("最高售价"),
        min("当日最低价").alias("最低售价"),
        avg("价格力星级").alias("平均价格力星级")
    ) \
        .withColumn("价格波动幅度", (col("最高售价") - col("最低售价")) / col("平均售价"))

    # 促销指标
    promotion_indicators = dws_marketing.groupBy("商品ID", "日期") \
        .agg(
        sum("促销投入金额").alias("总促销投入"),
        countDistinct("促销活动ID").alias("参与促销活动数"),
        sum("促销带来的销售额").alias("促销销售额"),
        sum("促销带来的销量").alias("促销销量")
    ) \
        .withColumn("促销ROI", col("促销销售额") / col("总促销投入")) \
        .withColumn("促销销售占比", col("促销销售额") / (col("促销销售额") + col("非促销销售额")))

    # 关联价格与促销指标
    price_promo_indicators = price_indicators \
        .join(promotion_indicators, ["商品ID", "日期"], "left") \
        .fillna(0, subset=["总促销投入", "参与促销活动数", "促销销售额", "促销销量"])

    print("不分层的价格与促销指标计算完成")
    return price_promo_indicators


def calculate_price_promotion_by_price_range(dws_price, dws_marketing):
    """
    按价格带分层计算价格与促销指标
    分析不同价格区间的商品在价格策略和促销效果上的差异
    """
    print("开始按价格带分层计算价格与促销指标...")

    # 基础指标
    base_indicators = calculate_price_promotion_indicators(dws_price, dws_marketing)

    # 按价格区间聚合
    price_range_summary = dws_price.groupBy("价格区间", "日期") \
        .agg(
        avg("当日均价").alias("区间平均价格"),
        avg("价格力星级").alias("区间平均价格力"),
        sum("促销带来的销售额").alias("区间促销总销售额")
    )

    # 关联价格区间数据
    range_indicators = base_indicators \
        .join(dws_price.select("商品ID", "日期", "价格区间").distinct(), ["商品ID", "日期"], "left") \
        .join(price_range_summary, ["价格区间", "日期"], "left") \
        .withColumn("价格带内价格排名",
                    row_number().over(Window.partitionBy("价格区间", "日期").orderBy(col("平均售价").desc()))) \
        .withColumn("区间促销占比", col("促销销售额") / col("区间促销总销售额"))

    print("按价格带分层的价格与促销指标计算完成")
    return range_indicators


# ---------------------- 4. 用户与评价指标计算 ----------------------

def calculate_user_rating_indicators(dws_rfm, dws_comment):
    """
    计算用户与评价指标（不分层方式）
    包含用户价值、评价得分、评价关键词等指标
    """
    print("开始计算不分层的用户与评价指标...")

    # RFM用户价值指标
    user_value = dws_rfm.groupBy("商品ID", "日期") \
        .agg(
        avg("r_score").alias("平均近度得分"),
        avg("f_score").alias("平均频次得分"),
        avg("m_score").alias("平均金额得分"),
        avg("rfm_total").alias("平均RFM总分")
    )

    # 评价指标
    rating_indicators = dws_comment.groupBy("商品ID", "日期") \
        .agg(
        avg("评分").alias("平均评分"),
        count("评价ID").alias("总评价数"),
        sum(when(col("评分") >= 4, 1).otherwise(0)).alias("好评数"),
        sum(when(col("评分") <= 2, 1).otherwise(0)).alias("差评数")
    ) \
        .withColumn("好评率", col("好评数") / col("总评价数")) \
        .withColumn("差评率", col("差评数") / col("总评价数"))

    # 关联用户价值与评价指标
    user_rating_indicators = user_value \
        .join(rating_indicators, ["商品ID", "日期"], "left")

    print("不分层的用户与评价指标计算完成")
    return user_rating_indicators


def calculate_user_rating_by_user_level(dws_rfm, dws_comment, dim_user):
    """
    按用户等级分层计算用户与评价指标
    分析不同等级用户对商品的评价和购买行为差异
    """
    print("开始按用户等级分层计算用户与评价指标...")

    # 关联用户等级信息
    rfm_with_level = dws_rfm \
        .join(dim_user.select("用户ID", "会员等级"), ["用户ID"], "left")

    # 按用户等级计算RFM指标
    level_rfm = rfm_with_level.groupBy("商品ID", "日期", "会员等级") \
        .agg(
        avg("r_score").alias("等级平均近度"),
        avg("f_score").alias("等级平均频次"),
        avg("m_score").alias("等级平均金额"),
        countDistinct("用户ID").alias("等级用户数")
    )

    # 关联用户等级和评价信息
    comment_with_level = dws_comment \
        .join(dim_user.select("用户ID", "会员等级"), ["用户ID"], "left")

    # 按用户等级计算评价指标
    level_rating = comment_with_level.groupBy("商品ID", "日期", "会员等级") \
        .agg(
        avg("评分").alias("等级平均评分"),
        count("评价ID").alias("等级评价总数"),
        sum(when(col("评分") >= 4, 1).otherwise(0)).alias("等级好评数")
    ) \
        .withColumn("等级好评率", col("等级好评数") / col("等级评价总数"))

    # 关联等级RFM和评价指标
    level_indicators = level_rfm \
        .join(level_rating, ["商品ID", "日期", "会员等级"], "left") \
        .fillna(0, subset=["等级评价总数", "等级好评数"])

    # 计算总体指标用于对比
    overall_indicators = calculate_user_rating_indicators(dws_rfm, dws_comment) \
        .select(
        "商品ID", "日期",
        col("平均评分").alias("总体平均评分"),
        col("好评率").alias("总体好评率")
    )

    # 关联总体指标，计算差异
    final_indicators = level_indicators \
        .join(overall_indicators, ["商品ID", "日期"], "left") \
        .withColumn("评分差异", col("等级平均评分") - col("总体平均评分")) \
        .withColumn("好评率差异", col("等级好评率") - col("总体好评率"))

    print("按用户等级分层的用户与评价指标计算完成")
    return final_indicators


# ---------------------- 5. SKU分析指标计算 ----------------------

def calculate_sku_indicators(dwd_sku, dim_product):
    """
    计算SKU指标（不分层方式）
    包含SKU销售表现、库存状况等指标
    """
    print("开始计算不分层的SKU指标...")

    # 计算SKU销售指标
    sku_sales = dwd_sku.groupBy("商品ID", "SKU ID", "日期") \
        .agg(
        sum("销售数量").alias("sku销量"),
        sum("销售金额").alias("sku销售额"),
        avg("销售单价").alias("sku平均售价"),
        last("库存余量").alias("当前库存")
    )

    # 计算商品总销量用于计算占比
    product_total = sku_sales.groupBy("商品ID", "日期") \
        .agg(
        sum("sku销量").alias("商品总销量"),
        sum("sku销售额").alias("商品总销售额")
    )

    # 关联商品总销量，计算SKU占比
    sku_indicators = sku_sales \
        .join(product_total, ["商品ID", "日期"], "left") \
        .withColumn("销量占比", col("sku销量") / col("商品总销量")) \
        .withColumn("销售额占比", col("sku销售额") / col("商品总销售额")) \
        .withColumn("库存周转率", col("sku销量") / col("当前库存")) \
        .join(dim_product.select("商品ID", "SKU ID", "SKU属性", "商品名称"), ["商品ID", "SKU ID"], "left")

    print("不分层的SKU指标计算完成")
    return sku_indicators


def calculate_sku_indicators_by_attribute(dwd_sku, dim_product):
    """
    按SKU属性分层计算SKU指标
    分析不同属性（如颜色、尺寸）的SKU表现差异
    """
    print("开始按SKU属性分层计算SKU指标...")

    # 获取基础SKU指标
    base_sku = calculate_sku_indicators(dwd_sku, dim_product)

    # 提取SKU属性（假设格式为"颜色:红色;尺寸:L"）
    attribute_indicators = base_sku \
        .withColumn("颜色", regexp_extract(col("SKU属性"), "颜色:([^;]+)", 1)) \
        .withColumn("尺寸", regexp_extract(col("SKU属性"), "尺寸:([^;]+)", 1))

    # 按颜色聚合
    color_summary = attribute_indicators.groupBy("商品ID", "日期", "颜色") \
        .agg(
        sum("sku销量").alias("颜色总销量"),
        sum("sku销售额").alias("颜色总销售额")
    )

    # 按尺寸聚合
    size_summary = attribute_indicators.groupBy("商品ID", "日期", "尺寸") \
        .agg(
        sum("sku销量").alias("尺寸总销量"),
        sum("sku销售额").alias("尺寸总销售额")
    )

    # 关联属性汇总数据
    final_indicators = attribute_indicators \
        .join(color_summary, ["商品ID", "日期", "颜色"], "left") \
        .join(size_summary, ["商品ID", "日期", "尺寸"], "left") \
        .withColumn("颜色销量占比", col("sku销量") / col("颜色总销量")) \
        .withColumn("尺寸销量占比", col("sku销量") / col("尺寸总销量"))

    print("按SKU属性分层的SKU指标计算完成")
    return final_indicators


# ---------------------- 数据写入函数 ----------------------

def write_to_mysql(df, table_name, mode="overwrite"):
    """将数据框写入MySQL数据库"""
    try:
        df.write \
            .format("jdbc") \
            .option("url", MYSQL_CONFIG["url"]) \
            .option("dbtable", table_name) \
            .option("user", MYSQL_CONFIG["user"]) \
            .option("password", MYSQL_CONFIG["password"]) \
            .option("driver", MYSQL_CONFIG["driver"]) \
            .mode(mode) \
            .save()
        print(f"{table_name}写入MySQL成功")
    except Exception as e:
        print(f"{table_name}写入MySQL失败: {str(e)}")


# ---------------------- 主函数 ----------------------

def main():
    """主函数，协调执行所有计算步骤"""
    # 创建Spark会话
    spark = create_spark_session()

    try:
        # 读取源数据
        dws_data, dwd_data, dim_data = read_source_data(spark)

        # 1. 计算商品核心指标
        core_indicators = calculate_core_indicators(dws_data["sales_summary"], dim_data["product"])
        write_to_mysql(core_indicators, "ads_product_core_indicators")

        core_by_category = calculate_core_indicators_by_category(
            dws_data["sales_summary"], dim_data["product"], dim_data["category"])
        write_to_mysql(core_by_category, "ads_product_core_by_category")

        # 2. 计算流量与转化指标
        traffic_indicators = calculate_traffic_indicators(
            dwd_data["page_view_detail"], dwd_data["cart_detail"], dwd_data["order_detail"])
        write_to_mysql(traffic_indicators, "ads_traffic_indicators")

        traffic_by_region = calculate_traffic_indicators_by_region(
            dwd_data["page_view_detail"], dwd_data["cart_detail"],
            dwd_data["order_detail"], dim_data["user"], dim_data["region"])
        write_to_mysql(traffic_by_region, "ads_traffic_by_region")

        # 3. 计算价格与促销指标
        price_promo = calculate_price_promotion_indicators(
            dws_data["price_analysis_summary"], dws_data["marketing_effect_summary"])
        write_to_mysql(price_promo, "ads_price_promotion_indicators")

        price_by_range = calculate_price_promotion_by_price_range(
            dws_data["price_analysis_summary"], dws_data["marketing_effect_summary"])
        write_to_mysql(price_by_range, "ads_price_by_range")

        # 4. 计算用户与评价指标
        user_rating = calculate_user_rating_indicators(
            dws_data["rfm_summary"], dws_data["comment_analysis_summary"])
        write_to_mysql(user_rating, "ads_user_rating_indicators")

        user_by_level = calculate_user_rating_by_user_level(
            dws_data["rfm_summary"], dws_data["comment_analysis_summary"], dim_data["user"])
        write_to_mysql(user_by_level, "ads_user_by_level")

        # 5. 计算SKU分析指标
        sku_indicators = calculate_sku_indicators(dwd_data["sku_sales_detail"], dim_data["product"])
        write_to_mysql(sku_indicators, "ads_sku_indicators")

        sku_by_attribute = calculate_sku_indicators_by_attribute(
            dwd_data["sku_sales_detail"], dim_data["product"])
        write_to_mysql(sku_by_attribute, "ads_sku_by_attribute")

        print("所有指标计算并写入完成")

    except Exception as e:
        print(f"执行过程中发生错误: {str(e)}")
    finally:
        # 停止Spark会话
        spark.stop()
        print("Spark会话已关闭")


if __name__ == "__main__":
    main()




