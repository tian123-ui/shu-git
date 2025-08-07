from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import os

# 配置环境
os.environ["PYSPARK_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("GenerateODSFromMock") \
    .getOrCreate()

# 工单编号注释：大数据-电商数仓-08-商品主题营销工具客服专属优惠看板

# 读取mock数据作为数据源
mock_df = spark.read \
    .option("header", "true") \
    .option("encoding", "UTF-8") \
    .csv("mock_data/mock.csv")

# ------------------------------
# 生成ODS层表：ods_customer_service_discount_mock
# 保留原始字段，仅添加数据加载时间作为ODS层标识
# ------------------------------
ods_df = mock_df.withColumn("ods_load_time", current_timestamp())

# 保存为ODS层表（保留原始结构，不做清洗）
output_ods = "ods_customer_service_discount_mock"
ods_df.write.mode("overwrite") \
    .option("header", "true") \
    .option("encoding", "UTF-8") \
    .csv(output_ods)

# 打印ODS层表结构，验证字段完整性
print("ODS层表字段如下：")
ods_df.printSchema()

# 后续层流转入口说明（无需执行代码，仅体现逻辑）：
# 1. DIM层：从ods_df提取用户ID、商品ID等关联维度信息，生成dim_user_info等维度表
# 2. DWD层：基于ods_df清洗无效值（如异常点击率），生成dwd_discount_detail等明细事实表
# 3. DWS层：对DWD层数据按活动/客服维度汇总，生成send_cnt、pay_amt等指标汇总表
# 4. ADS层：从DWS层提取指标，生成ads_dashboard_data供看板展示

# 停止SparkSession
spark.stop()

