from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
import os
import random
from datetime import datetime, timedelta
from decimal import Decimal

# 工单编号：大数据 - 电商数仓 - 08 - 商品主题营销工具客服专属优惠看板


# 配置 PySpark 环境
os.environ["PYSPARK_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("生成700条ODS层数据（每表100条）") \
    .master("local[*]") \
    .getOrCreate()

# 创建存储目录
output_root = "mock_data/ods"
if not os.path.exists(output_root):
    os.makedirs(output_root)


# 生成随机日期工具函数
def random_date(start, end):
    return (start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))).strftime(
        "%Y-%m-%d %H:%M:%S")


# 基础参数设置（扩展以支持每表100条数据）
start_date = datetime(2024, 12, 1)
end_date = datetime(2025, 3, 31)
shop_id = "shop_001"
activity_levels = ["商品级", "SKU级"]
promo_types = ["固定优惠", "自定义优惠"]
status_list = ["进行中", "已结束", "未开始"]
cs_ids = [f"cs_{i:03d}" for i in range(1, 21)]  # 20个客服
user_ids = [f"user_{i:03d}" for i in range(1, 301)]  # 300个用户
product_ids = [f"prod_{i:03d}" for i in range(1, 151)]  # 150个商品
sku_ids = [f"sku_{i:03d}" for i in range(1, 301)]  # 300个SKU

# ------------------------------
# 1. 活动表：ods_customer_service_promo_activity（100条）
# ------------------------------
activity_schema = StructType([
    StructField("activity_id", StringType(), False),
    StructField("activity_name", StringType(), False),
    StructField("activity_level", StringType(), False),
    StructField("promo_type", StringType(), False),
    StructField("custom_promo_max", IntegerType(), True),
    StructField("start_time", StringType(), False),
    StructField("end_time", StringType(), False),
    StructField("status", StringType(), False),
    StructField("shop_id", StringType(), False),
    StructField("create_time", StringType(), False),
    StructField("raw_extra", StringType(), True)
])

activity_data = []
for i in range(1, 101):  # 生成100条活动数据
    act_id = f"act_{i:03d}"
    level = random.choice(activity_levels)
    p_type = random.choice(promo_types)
    custom_max = random.randint(5, 50) if p_type == "自定义优惠" else None
    # 活动时长2-120天
    start = random_date(start_date, end_date - timedelta(days=2))
    start_dt = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
    end_dt = start_dt + timedelta(days=random.randint(2, 120))
    end = end_dt.strftime("%Y-%m-%d %H:%M:%S")
    # 状态逻辑
    status = "已结束" if end_dt < datetime.now() else "进行中" if start_dt < datetime.now() else "未开始"
    # 活动名称≤10字
    names = ["店庆特惠", "新客立减", "老客回馈", "限时优惠", "节日促销", "清仓活动", "会员专享",
             "换季折扣", "新品特惠", "组合优惠", "满减活动", "秒杀活动"]
    activity_data.append((
        act_id, random.choice(names), level, p_type, custom_max,
        start, end, status, shop_id,
        (start_dt - timedelta(days=random.randint(1, 7))).strftime("%Y-%m-%d %H:%M:%S"),
        f'{{"creator":"{random.choice(["运营部", "客服部"])}"}}'
    ))

activity_df = spark.createDataFrame(activity_data, activity_schema)
activity_df.write.mode("overwrite").option("header", "true").csv(f"{output_root}/ods_customer_service_promo_activity")

# ------------------------------
# 2. 活动-商品关联表：ods_customer_service_promo_activity_item（100条）
# ------------------------------
item_schema = StructType([
    StructField("id", StringType(), False),
    StructField("activity_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("sku_id", StringType(), True),
    StructField("promo_amount", IntegerType(), False),
    StructField("limit_purchase_count", IntegerType(), False),
    StructField("is_removed", StringType(), False),
    StructField("create_time", StringType(), False)
])

item_data = []
for i in range(1, 101):  # 生成100条关联数据
    item_id = f"item_{i:03d}"
    act_id = f"act_{random.randint(1, 100):03d}"  # 关联100个活动
    prod_id = random.choice(product_ids[:100])  # 从150个商品中选100个
    # 商品级活动无SKU
    act_level = [d[2] for d in activity_data if d[0] == act_id][0]
    sku_id = random.choice(sku_ids) if act_level == "SKU级" else None
    # 优惠金额≤5000且为整数
    promo_amt = random.randint(1, 200)
    # 限购次数默认1-5次
    limit_cnt = random.randint(1, 5)
    is_removed = "是" if random.random() < 0.1 else "否"  # 10%概率已移出
    create_t = random_date(start_date, datetime.now())
    item_data.append((item_id, act_id, prod_id, sku_id, promo_amt, limit_cnt, is_removed, create_t))

item_df = spark.createDataFrame(item_data, item_schema)
item_df.write.mode("overwrite").option("header", "true").csv(f"{output_root}/ods_customer_service_promo_activity_item")

# ------------------------------
# 3. 优惠发送表：ods_customer_service_promo_send（100条）
# ------------------------------
send_schema = StructType([
    StructField("send_id", StringType(), False),
    StructField("activity_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("sku_id", StringType(), True),
    StructField("customer_service_id", StringType(), False),
    StructField("consumer_id", StringType(), False),
    StructField("actual_promo_amount", IntegerType(), False),
    StructField("valid_duration", IntegerType(), False),
    StructField("send_time", StringType(), False),
    StructField("remark", StringType(), True)
])

send_data = []
remarks = ["咨询未下单", "价格敏感", "新客引导", "老客召回", "活动推广",
           "主动咨询", "售后推荐", "关联销售", "复购激励", "流失挽回"]
for i in range(1, 101):  # 生成100条发送数据
    send_id = f"send_{i:03d}"
    act_id = f"act_{random.randint(1, 100):03d}"
    # 关联商品 - 确保能找到有效的关联项
    related_items = [d for d in item_data if d[1] == act_id and d[6] == "否"]
    if not related_items:  # 如果没有有效的关联项，放宽条件
        related_items = [d for d in item_data if d[1] == act_id]
    item = random.choice(related_items) if related_items else random.choice(item_data)
    prod_id, sku_id = item[2], item[3]
    # 实际优惠金额
    act_type = [d[3] for d in activity_data if d[0] == act_id][0]
    if act_type == "固定优惠":
        actual_amt = item[4]
    else:
        max_amt = [d[4] for d in activity_data if d[0] == act_id][0]
        actual_amt = random.randint(1, max_amt)
    # 有效期1-24小时
    duration = random.randint(1, 24)
    # 发送时间在活动期内
    act_start = [d[5] for d in activity_data if d[0] == act_id][0]
    act_end = [d[6] for d in activity_data if d[0] == act_id][0]
    send_t = random_date(datetime.strptime(act_start, "%Y-%m-%d %H:%M:%S"),
                         datetime.strptime(act_end, "%Y-%m-%d %H:%M:%S"))
    send_data.append((
        send_id, act_id, prod_id, sku_id, random.choice(cs_ids),
        random.choice(user_ids), actual_amt, duration, send_t,
        random.choice(remarks) if random.random() < 0.7 else None
    ))

send_df = spark.createDataFrame(send_data, send_schema)
send_df.write.mode("overwrite").option("header", "true").csv(f"{output_root}/ods_customer_service_promo_send")

# ------------------------------
# 4. 优惠核销表：ods_customer_service_promo_use（100条）
# ------------------------------
use_schema = StructType([
    StructField("use_id", StringType(), False),
    StructField("send_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("pay_time", StringType(), False),
    StructField("pay_amount", DecimalType(10, 2), False),
    StructField("purchase_count", IntegerType(), False)
])

use_data = []
for i in range(1, 101):  # 生成100条核销数据
    use_id = f"use_{i:03d}"
    # 确保选择的send_id有对应的有效关联项
    valid = False
    while not valid:
        send_id = f"send_{random.randint(1, 100):03d}"
        send_rec = [d for d in send_data if d[0] == send_id]
        if send_rec:
            send_rec = send_rec[0]
            # 检查是否有匹配的item_data
            matching_items = [d for d in item_data if d[2] == send_rec[2] and d[1] == send_rec[1]]
            if matching_items:
                valid = True

    # 关联发送记录
    send_t = datetime.strptime(send_rec[8], "%Y-%m-%d %H:%M:%S")
    duration = send_rec[7]
    # 核销时间在有效期内（支持跨天）
    pay_t = random_date(send_t, send_t + timedelta(hours=duration))
    # 支付金额（转为Decimal类型）
    pay_amt = Decimal(str(round(random.uniform(50, 1000), 2)))
    # 购买数量≤限购次数
    item_id = [d[0] for d in item_data if d[2] == send_rec[2] and d[1] == send_rec[1]][0]
    limit_cnt = [d[5] for d in item_data if d[0] == item_id][0]
    purchase_cnt = random.randint(1, limit_cnt)
    use_data.append((
        use_id, send_id, f"order_{i:03d}", pay_t, pay_amt, purchase_cnt
    ))

use_df = spark.createDataFrame(use_data, use_schema)
use_df.write.mode("overwrite").option("header", "true").csv(f"{output_root}/ods_customer_service_promo_use")

# ------------------------------
# 5. 商品表：ods_product（100条）
# ------------------------------
product_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("price", DecimalType(10, 2), False),
    StructField("shop_id", StringType(), False),
    StructField("status", StringType(), False)
])

product_data = []
prod_names = ["卫衣", "牛仔裤", "运动鞋", "T恤", "外套", "连衣裙", "背包", "手表",
              "手机壳", "充电器", "耳机", "裤子", "衬衫", "毛衣", "鞋子", "帽子"]
for pid in product_ids[:100]:  # 生成100条商品数据
    # 价格转换为Decimal类型
    price = Decimal(str(round(random.uniform(50, 1000), 2)))
    product_data.append((
        pid, f"{random.choice(prod_names)}{random.randint(101, 999)}",
        price, shop_id,
        "在售" if random.random() < 0.8 else "下架"
    ))

product_df = spark.createDataFrame(product_data, product_schema)
product_df.write.mode("overwrite").option("header", "true").csv(f"{output_root}/ods_product")

# ------------------------------
# 6. SKU表：ods_sku（100条）
# ------------------------------
sku_schema = StructType([
    StructField("sku_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("spec", StringType(), False),
    StructField("sku_price", DecimalType(10, 2), False)
])

sku_data = []
specs = ["红色-S", "红色-M", "红色-L", "黑色-S", "黑色-M", "黑色-L",
         "蓝色-S", "蓝色-M", "蓝色-L", "白色-S", "白色-M", "白色-L",
         "灰色-S", "灰色-M", "灰色-L"]
for sid in sku_ids[:100]:  # 生成100条SKU数据
    prod_id = random.choice(product_ids[:100])  # 关联100个商品
    # 从商品表数据中获取基础价格（已为Decimal类型）
    base_price = [d[2] for d in product_data if d[0] == prod_id][0]
    # 计算SKU价格并保持Decimal类型
    sku_price = Decimal(str(round(float(base_price) * (1 + random.uniform(-0.1, 0.2)), 2)))
    sku_data.append((sid, prod_id, random.choice(specs), sku_price))

sku_df = spark.createDataFrame(sku_data, sku_schema)
sku_df.write.mode("overwrite").option("header", "true").csv(f"{output_root}/ods_sku")

# ------------------------------
# 7. 客服表：ods_customer_service（100条）
# ------------------------------
cs_schema = StructType([
    StructField("customer_service_id", StringType(), False),
    StructField("cs_name", StringType(), False),
    StructField("shop_id", StringType(), False)
])

cs_data = []
cs_names = ["张三", "李四", "王五", "赵六", "孙七", "周八", "吴九", "郑十",
            "钱一", "孙二", "周叁", "吴四", "郑五", "王六", "赵七", "孙八",
            "周九", "吴十", "郑一", "钱二"]
# 扩展客服ID以支持100条数据
cs_ids_extended = [f"cs_{i:03d}" for i in range(1, 101)]
for cid in cs_ids_extended:  # 生成100条客服数据
    cs_data.append((cid, random.choice(cs_names), shop_id))

cs_df = spark.createDataFrame(cs_data, cs_schema)
cs_df.write.mode("overwrite").option("header", "true").csv(f"{output_root}/ods_customer_service")

print(f"已生成700条数据至 {os.path.abspath(output_root)}，包含7张ODS表，每张表100条数据")
spark.stop()



# 表结构设计：严格按照文档中客服专属优惠的业务流程，设计了 7 张核心 ODS 表，包括活动表、活动 - 商品关联表、优惠发送表、优惠核销表等，覆盖了从活动创建到优惠使用的全链路数据🔶3-20🔶3-23🔶3-29。
# 业务规则适配：
# 活动名称限制在 10 字以内，活动时长设置为 2-120 天，符合文档中活动创建的规则🔶3-80。
# 优惠金额设计为整数且不超过 5000 元，自定义优惠金额不超过设置的上限，遵循了文档中对优惠金额的限制🔶3-99🔶3-100🔶3-88。
# 优惠有效期设置为 1-24 小时，支持跨天核销场景，与文档中 “支付次数可能大于发送次数” 的说明一致🔶3-41🔶3-206。
# 数据关联性：通过activity_id关联活动表与活动 - 商品关联表，通过send_id关联优惠发送表与核销表，确保了数据链路的完整性，满足后续分层处理时的关联需求🔶3-37🔶3-38🔶3-40。
# 存储格式：所有表以 CSV 格式存储在mock_data目录下，包含表头，保留了原始数据格式（如时间字段为字符串类型），符合 ODS 层 “原始性” 的特点🔶3-235。
#
# 该代码生成的 700 条数据（每表 100 条）可作为数仓 ODS 层的基础数据，为后续 DIM、DWD 等分层的清洗转换提供可靠输入。

