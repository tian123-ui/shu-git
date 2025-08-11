from pyspark.sql import SparkSession
import pandas as pd
import datetime
import os

# 工单编号：大数据 - 电商数仓 - 08 - 商品主题营销工具客服专属优惠看板

class PromotionDashboardTester:
    def __init__(self, spark, results_dir):
        self.spark = spark
        self.results_dir = results_dir  # 结果文件所在目录
        self.test_results = []
        self.test_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.registered_views = []  # 已注册的视图列表

    def log_result(self, test_id, test_name, sql, expected, actual, result):
        """记录测试结果"""
        self.test_results.append({
            "测试ID": test_id,
            "测试名称": test_name,
            "测试SQL": sql,
            "预期结果": expected,
            "实际结果": actual,
            "测试结果": result,
            "测试时间": self.test_date
        })

    def register_csv_as_view(self, csv_file, view_name):
        """将CSV文件注册为Spark临时视图"""
        try:
            file_path = os.path.join(self.results_dir, csv_file)
            if not os.path.exists(file_path):
                print(f"警告: 文件不存在 - {file_path}")
                return False

            # 读取CSV文件
            df = self.spark.read.csv(
                file_path,
                header=True,
                inferSchema=True
            )

            # 注册为临时视图
            df.createOrReplaceTempView(view_name)
            self.registered_views.append(view_name)
            print(f"已注册视图: {view_name} (来自文件: {csv_file})")
            return True
        except Exception as e:
            print(f"注册视图 {view_name} 失败: {str(e)}")
            return False

    def test_view_existence(self, view_mappings):
        """验证必要视图是否存在"""
        for idx, (csv_file, view_name) in enumerate(view_mappings.items(), 1):
            test_id = f"TB-{idx:02d}"
            test_name = f"验证视图{view_name}存在性"

            if view_name in self.registered_views:
                self.log_result(
                    test_id, test_name,
                    f"注册视图 {view_name} 来自 {csv_file}",
                    "视图存在", "视图存在", "通过"
                )
            else:
                self.log_result(
                    test_id, test_name,
                    f"注册视图 {view_name} 来自 {csv_file}",
                    "视图存在", "视图不存在", "失败"
                )

    def test_overview_consistency(self):
        """验证概览指标一致性"""
        # 总发送量一致性
        test_id = "OV-01"
        test_name = "总发送量分层与不分层一致性"
        sql_non_layered = """
            SELECT total_send_count 
            FROM ads_customer_service_promo_overview_non_layered 
            WHERE period_type='all'
        """
        sql_layered = """
            SELECT SUM(total_send_count) 
            FROM ads_customer_service_promo_overview_layered
        """

        try:
            non_layered = self.spark.sql(sql_non_layered).collect()[0][0] or 0
            layered = self.spark.sql(sql_layered).collect()[0][0] or 0
            # 考虑浮点数精度问题，使用近似比较
            result = "通过" if abs(non_layered - layered) < 0.001 else "失败"
            self.log_result(
                test_id, test_name,
                f"不分层SQL: {sql_non_layered}\n分层SQL: {sql_layered}",
                f"相等({non_layered})",
                f"{non_layered} vs {layered}",
                result
            )
        except Exception as e:
            self.log_result(test_id, test_name, f"SQL执行错误: {str(e)}", "无", "无", "失败")

    def test_activity_consistency(self):
        """验证活动效果指标一致性"""
        test_id = "AC-01"
        test_name = "活动发送量分层与不分层一致性"
        sql = """
            SELECT 
                a.activity_id, 
                a.send_count as non_layered_send,
                b.total_send as layered_send
            from ads_customer_service_promo_activity_effect_non_layered a
            join (
                select activity_id, sum(send_count) total_send
                from ads_customer_service_promo_activity_effect_layered
                group by activity_id
            ) b on a.activity_id = b.activity_id
            where abs(a.send_count - b.total_send) > 0.001
        """

        try:
            diff_count = self.spark.sql(sql).count()
            result = "通过" if diff_count == 0 else "失败"
            self.log_result(
                test_id, test_name, sql,
                "0条差异记录",
                f"{diff_count}条差异记录",
                result
            )
        except Exception as e:
            self.log_result(test_id, test_name, f"SQL执行错误: {str(e)}", "无", "无", "失败")

    def test_customer_service_consistency(self):
        """验证客服绩效指标一致性"""
        test_id = "CS-01"
        test_name = "客服转化率分层与不分层一致性"
        sql = """
            select 
                abs(
                    sum(a.use_count)/nullif(sum(a.send_count),0) - 
                    sum(b.use_count)/nullif(sum(b.send_count),0)
                ) as conv_diff
            from ads_customer_service_performance_non_layered a
            join (
                select 
                    customer_service_id, 
                    sum(send_count) send_count, 
                    sum(use_count) use_count
                from ads_customer_service_performance_layered
                group by customer_service_id
            ) b on a.customer_service_id = b.customer_service_id
        """

        try:
            diff = self.spark.sql(sql).collect()[0][0] or 0
            result = "通过" if diff < 0.001 else "失败"
            self.log_result(
                test_id, test_name, sql,
                "差异<0.001",
                f"差异值: {diff}",
                result
            )
        except Exception as e:
            self.log_result(test_id, test_name, f"SQL执行错误: {str(e)}", "无", "无", "失败")

    def test_rfm_logic(self):
        """验证RFM分层逻辑正确性"""
        test_id = "RF-01"
        test_name = "RFM分层规则验证"
        sql = """
            select count(*) as error_count
            from dws_customer_rfm_layer
            where 
                (rfm_total > 12 and user_level != '高价值用户') or
                (rfm_total > 8 and rfm_total <= 12 and user_level != '潜力用户') or
                (rfm_total > 5 and rfm_total <= 8 and user_level != '一般用户') or
                (rfm_total <= 5 and user_level != '流失用户')
        """

        try:
            error_count = self.spark.sql(sql).collect()[0][0] or 0
            result = "通过" if error_count == 0 else "失败"
            self.log_result(
                test_id, test_name, sql,
                "0条错误分层记录",
                f"{error_count}条错误分层记录",
                result
            )
        except Exception as e:
            self.log_result(test_id, test_name, f"SQL执行错误: {str(e)}", "无", "无", "失败")

    def generate_test_report(self, output_path="test_report.csv"):
        """生成测试报告"""
        df = pd.DataFrame(self.test_results)
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"测试报告已生成: {output_path}")
        return df


if __name__ == "__main__":
    # 初始化Spark会话
    spark = SparkSession.builder \
        .appName("PromotionDashboardTest") \
        .master("local[*]") \
        .getOrCreate()

    # 结果文件所在目录（与主程序输出目录一致）
    results_dir = "promotion_results"

    # 初始化测试器
    tester = PromotionDashboardTester(spark, results_dir)

    # 定义CSV文件与视图名的映射关系
    view_mappings = {
        "ads_customer_service_performance_layered_merged.csv":
            "ads_customer_service_performance_layered",
        "ads_customer_service_performance_non_layered_merged.csv":
            "ads_customer_service_performance_non_layered",
        "ads_customer_service_promo_activity_effect_layered_merged.csv":
            "ads_customer_service_promo_activity_effect_layered",
        "ads_customer_service_promo_activity_effect_non_layered_merged.csv":
            "ads_customer_service_promo_activity_effect_non_layered",
        "ads_customer_service_promo_overview_layered_merged.csv":
            "ads_customer_service_promo_overview_layered",
        "ads_customer_service_promo_overview_non_layered_merged.csv":
            "ads_customer_service_promo_overview_non_layered",
        "ads_customer_service_promo_time_trend_non_layered_merged.csv":
            "ads_customer_service_promo_time_trend_non_layered",
        "dws_customer_rfm_layer_merged.csv":
            "dws_customer_rfm_layer"
    }

    # 注册所有CSV文件为临时视图
    print("开始注册视图...")
    for csv_file, view_name in view_mappings.items():
        tester.register_csv_as_view(csv_file, view_name)

    # 执行测试
    print("\n开始执行测试...")
    tester.test_view_existence(view_mappings)
    tester.test_overview_consistency()
    tester.test_activity_consistency()
    tester.test_customer_service_consistency()
    tester.test_rfm_logic()

    # 生成报告
    report = tester.generate_test_report()
    print("\n测试 summary:")
    print(report[["测试ID", "测试名称", "测试结果"]].to_string(index=False))

    # 关闭Spark会话
    spark.stop()
