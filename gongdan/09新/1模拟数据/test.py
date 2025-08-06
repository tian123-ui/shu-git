
#TODO: 3、店内路径看板测试文档需含测试记录及测试sql，同时分层、不分层两种实现方式的数据需一致

import pytest
import pandas as pd
import numpy as np
import os
import csv
import json
from datetime import datetime
import shutil


# ------------------------------
# 模拟数据处理和看板类（使用实际字段名）
# ------------------------------
class MockDataProcessor:
    """模拟数据处理器，用于模拟实际环境中的数据加载和处理过程"""

    def __init__(self, data_dir):
        """
        初始化数据处理器
        :param data_dir: 数据存放目录
        """
        self.data_dir = data_dir  # 数据目录路径
        self.processed_data = None  # 用于存储处理后的数据

    def full_process(self):
        """模拟完整的数据处理流程（加载、清洗、转换等）"""
        print(f"从 {self.data_dir} 加载并处理数据")
        self.processed_data = "mock_processed_data"  # 模拟处理结果


class MockPathDashboard:
    """模拟店内路径看板，用于生成测试所需的指标数据"""

    def __init__(self, data_processor):
        """
        初始化路径看板
        :param data_processor: 数据处理器实例，用于获取处理后的数据
        """
        self.data_processor = data_processor  # 数据处理器引用

    def get_raw_metrics(self, date_start, date_end):
        """
        模拟获取不分层的原始指标数据（整体数据，不做维度拆分）
        :param date_start: 开始日期
        :param date_end: 结束日期
        :return: 包含各类指标的字典，值为DataFrame
        """
        return {
            # 终端指标：访客数和订单数
            'terminal_metrics': pd.DataFrame({
                'visitor_count': [1000, 2000],  # 实际字段名：访客数
                'order_count': [100, 150]  # 实际字段名：订单数
            }),
            # 页面指标：浏览量
            'page_metrics': pd.DataFrame({
                'page_views': [5000, 8000]  # 实际字段名：浏览量
            }),
            # 页面排名：页面名称和对应的排名
            'page_rank': pd.DataFrame({
                'page_name': [f'页面{i}' for i in range(1, 11)],  # 页面名称
                'rank': list(range(1, 11))  # 实际字段名：排名
            }),
            # 路径指标：来源页面、去向页面和对应的访客数
            'path_metrics': pd.DataFrame({
                'source_page': [f'页面{i // 2}' for i in range(15)],  # 来源页面
                'dest_page': [f'页面{i % 10 + 1}' for i in range(15)],  # 去向页面
                'visitor_count': [150 - i * 10 for i in range(15)]  # 实际字段名：访客数
            }),
            # 入口页面：页面名称和对应的访客数
            'entry_pages': pd.DataFrame({
                'page_name': [f'入口页面{i}' for i in range(1, 6)],  # 页面名称
                'visitor_count': [500, 400, 300, 200, 100]  # 实际字段名：访客数
            })
        }

    def get_layered_metrics(self, time_granularity='day'):
        """
        模拟获取分层的指标数据（按维度拆分后的数据）
        :param time_granularity: 时间粒度，默认为'day'（按天）
        :return: 包含各类指标的字典，值为DataFrame
        """
        return {
            # 终端指标：访客数和订单数（与原始数据略有差异，模拟分层处理的影响）
            'terminal_metrics': pd.DataFrame({
                'visitor_count': [1001, 1999],  # 实际字段名：访客数
                'order_count': [99, 151]  # 实际字段名：订单数
            }),
            # 页面指标：浏览量（与原始数据略有差异）
            'page_metrics': pd.DataFrame({
                'page_views': [5005, 7995]  # 实际字段名：浏览量
            }),
            # 页面排名：部分页面名称有变化，模拟分层后的差异
            'page_rank': pd.DataFrame({
                'page_name': [f'页面{i}' if i not in [3, 7] else f'页面{i + 10}' for i in range(1, 11)],
                'rank': list(range(1, 11))  # 实际字段名：排名
            }),
            # 路径指标：部分页面名称有变化，访客数有微小差异
            'path_metrics': pd.DataFrame({
                'source_page': [f'页面{i // 2}' if i not in [5, 9, 12] else f'页面{20 + i}' for i in range(15)],
                'dest_page': [f'页面{i % 10 + 1}' if i not in [5, 9, 12] else f'页面{20 + i}' for i in range(15)],
                'visitor_count': [150 - i * 10 + (0 if i not in [3, 7] else 5) for i in range(15)]  # 实际字段名：访客数
            }),
            # 入口页面：部分页面名称有变化，访客数有微小差异
            'entry_pages': pd.DataFrame({
                'page_name': [f'入口页面{i}' if i not in [2, 4] else f'入口页面{i + 10}' for i in range(1, 6)],
                'visitor_count': [502, 398, 305, 197, 103]  # 实际字段名：访客数
            })
        }


# ------------------------------
# 测试配置（使用实际字段名映射）
# ------------------------------
TEST_CONFIG = {
    "data_dir": "ecommerce_data",  # 测试数据目录
    "result_dir": "ecommerce_test",  # 测试结果输出目录
    # 测试时间周期
    "test_period": {"start": "2025-01-01", "end": "2025-01-30"},
    # 测试容差配置
    "tolerance": {
        "numeric": 5,  # 数值型指标的绝对容差
        "conversion": 0.05,  # 转化率的相对容差（5%）
        "rank_match": 0.7  # 排名匹配度的容差（70%）
    },
    # 字段名映射：程序变量名 -> 实际SQL字段名
    "field_mapping": {
        "total_visitors": "visitor_count",  # 总访客数对应实际字段
        "page_views": "page_views",  # 浏览量对应实际字段
        "order_count": "order_count",  # 订单数对应实际字段
        "rank": "rank"  # 排名对应实际字段
    },
    # 测试用例清单
    "test_cases": [
        {"id": "TC001", "name": "总访客数一致性验证"},
        {"id": "TC002", "name": "总浏览量一致性验证"},
        {"id": "TC003", "name": "下单转化率一致性验证"},
        {"id": "TC004", "name": "TOP10页面排名一致性验证"},
        {"id": "TC005", "name": "主要页面流转路径一致性验证"},
        {"id": "TC006", "name": "入口页面分布一致性验证"}
    ]
}


# ------------------------------
# 测试辅助函数（生成正确字段名的SQL）
# ------------------------------
def init_test_env():
    """初始化测试环境，清理旧结果并创建必要目录"""
    # 清理现有测试结果目录
    if os.path.exists(TEST_CONFIG["result_dir"]):
        for filename in os.listdir(TEST_CONFIG["result_dir"]):
            file_path = os.path.join(TEST_CONFIG["result_dir"], filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)  # 删除文件或链接
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)  # 删除目录
            except Exception as e:
                print(f"清理目录时出错: {e}")
    else:
        os.makedirs(TEST_CONFIG["result_dir"])  # 创建新目录

    # 创建测试记录CSV文件并写入表头
    record_path = os.path.join(TEST_CONFIG["result_dir"], "test_records.csv")
    with open(record_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            "测试用例ID", "测试用例名称", "测试时间", "测试结果",
            "分层数据值", "不分层数据值", "绝对差异", "相对差异(%)", "备注"
        ])
    return record_path


def generate_test_sql():
    """生成用于验证数据一致性的SQL脚本（使用实际字段名）"""
    # 从配置中获取实际字段名
    visitor_field = TEST_CONFIG["field_mapping"]["total_visitors"]
    page_view_field = TEST_CONFIG["field_mapping"]["page_views"]

    # SQL内容，包含分层和不分层两种查询方式
    sql_content = f"""-- 店内路径看板分层与不分层数据一致性测试SQL
-- 测试周期: {TEST_CONFIG['test_period']['start']} 至 {TEST_CONFIG['test_period']['end']}
-- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- 使用实际字段名: {visitor_field}(总访客数), {page_view_field}(浏览量)

-- =============================================
-- TC001: 总访客数一致性验证
-- =============================================
-- 分层方式（使用实际字段名）
SELECT SUM({visitor_field}) AS 分层总访客数
FROM ads_page_visit_rank
WHERE time_dimension BETWEEN '{TEST_CONFIG['test_period']['start']}' AND '{TEST_CONFIG['test_period']['end']}';

-- 不分层方式
SELECT COUNT(DISTINCT visitor_id) AS 不分层总访客数
FROM ods_page_behavior
WHERE stat_date BETWEEN '{TEST_CONFIG['test_period']['start']}' AND '{TEST_CONFIG['test_period']['end']}';

-- =============================================
-- TC002: 总浏览量一致性验证
-- =============================================
-- 分层方式（使用实际字段名）
SELECT SUM({page_view_field}) AS 分层总浏览量
FROM ads_page_visit_rank
WHERE time_dimension BETWEEN '{TEST_CONFIG['test_period']['start']}' AND '{TEST_CONFIG['test_period']['end']}';

-- 不分层方式
SELECT COUNT(*) AS 不分层总浏览量
FROM ods_page_behavior
WHERE stat_date BETWEEN '{TEST_CONFIG['test_period']['start']}' AND '{TEST_CONFIG['test_period']['end']}';
"""

    # 保存SQL脚本到文件
    sql_path = os.path.join(TEST_CONFIG["result_dir"], "test_verification.sql")
    with open(sql_path, "w", encoding="utf-8") as f:
        f.write(sql_content)
    return sql_path


# ------------------------------
# 测试执行类（使用实际字段名）
# ------------------------------
class TestPathConsistency:
    """测试店内路径看板的分层与不分层数据一致性"""

    @classmethod
    def setup_class(cls):
        """测试类初始化：创建测试环境和准备测试数据"""
        cls.record_path = init_test_env()  # 初始化测试环境并获取记录文件路径

        try:
            # 创建数据处理器并执行处理流程
            cls.data_processor = MockDataProcessor(TEST_CONFIG["data_dir"])
            cls.data_processor.full_process()

            # 创建看板实例
            cls.dashboard = MockPathDashboard(cls.data_processor)

            # 获取两种方式的指标数据（不分层和分层）
            cls.raw_metrics = cls.dashboard.get_raw_metrics(
                date_start=TEST_CONFIG["test_period"]["start"],
                date_end=TEST_CONFIG["test_period"]["end"]
            )
            cls.layered_metrics = cls.dashboard.get_layered_metrics(time_granularity='day')

            # 保存指标数据到JSON文件，用于后续分析
            with open(os.path.join(TEST_CONFIG["result_dir"], "raw_metrics.json"), 'w') as f:
                json.dump({k: v.to_json() for k, v in cls.raw_metrics.items()}, f, ensure_ascii=False)
            with open(os.path.join(TEST_CONFIG["result_dir"], "layered_metrics.json"), 'w') as f:
                json.dump({k: v.to_json() for k, v in cls.layered_metrics.items()}, f, ensure_ascii=False)

        except Exception as e:
            print(f"测试初始化失败: {str(e)}")
            raise  # 抛出异常，终止测试

    @classmethod
    def teardown_class(cls):
        """测试类结束处理：生成测试报告和验证SQL"""
        sql_path = generate_test_sql()
        print(f"\n测试完成! 结果保存至: {TEST_CONFIG['result_dir']}")
        print(f"测试记录: {cls.record_path}")
        print(f"验证SQL: {sql_path}")

    def record_result(self, case_id, case_name, result, layered_val, raw_val, notes=""):
        """
        记录测试结果到CSV文件
        :param case_id: 测试用例ID
        :param case_name: 测试用例名称
        :param result: 测试结果（True/False）
        :param layered_val: 分层数据值
        :param raw_val: 不分层数据值
        :param notes: 备注信息
        """
        abs_diff = ""  # 绝对差异
        rel_diff = ""  # 相对差异

        try:
            # 计算数值差异
            layered_num = float(layered_val)
            raw_num = float(raw_val)
            abs_diff = f"{layered_num - raw_num:.4f}"
            if raw_num != 0:
                rel_diff = f"{abs(layered_num - raw_num) / raw_num * 100:.2f}"
            else:
                rel_diff = "N/A"  # 避免除以零
        except:
            abs_diff = "无法计算"
            rel_diff = "无法计算"

        # 写入测试记录
        with open(self.record_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                case_id, case_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "通过" if result else "失败",
                layered_val, raw_val, abs_diff, rel_diff, notes
            ])

    # ------------------------------
    # 测试用例（使用实际字段名）
    # ------------------------------
    def test_total_visitors(self):
        """TC001: 总访客数一致性验证（使用实际字段visitor_count）"""
        # 获取当前测试用例信息
        case = next(c for c in TEST_CONFIG["test_cases"] if c["id"] == "TC001")
        visitor_field = TEST_CONFIG["field_mapping"]["total_visitors"]  # 获取实际字段名

        try:
            # 计算分层和不分层的总访客数
            layered_total = self.layered_metrics['terminal_metrics'][visitor_field].sum()
            raw_total = self.raw_metrics['terminal_metrics'][visitor_field].sum()

            # 验证是否在容差范围内
            result = np.isclose(layered_total, raw_total, atol=TEST_CONFIG["tolerance"]["numeric"])

            # 记录测试结果
            self.record_result(
                case["id"], case["name"], result,
                f"{layered_total:.0f}", f"{raw_total:.0f}",
                f"使用字段: {visitor_field}"
            )

            # 断言验证结果
            assert result, f"总访客数不一致: 分层={layered_total}, 不分层={raw_total} (字段: {visitor_field})"
        except Exception as e:
            self.record_result(case["id"], case["name"], False, "N/A", "N/A", str(e))
            assert False, f"测试失败: {str(e)}"

    def test_total_page_views(self):
        """TC002: 总浏览量一致性验证（使用实际字段page_views）"""
        case = next(c for c in TEST_CONFIG["test_cases"] if c["id"] == "TC002")
        page_view_field = TEST_CONFIG["field_mapping"]["page_views"]  # 获取实际字段名

        try:
            # 计算分层和不分层的总浏览量
            layered_pv = self.layered_metrics['page_metrics'][page_view_field].sum()
            raw_pv = self.raw_metrics['page_metrics'][page_view_field].sum()

            # 验证是否在容差范围内
            result = np.isclose(layered_pv, raw_pv, atol=TEST_CONFIG["tolerance"]["numeric"])

            # 记录测试结果
            self.record_result(
                case["id"], case["name"], result,
                f"{layered_pv:.0f}", f"{raw_pv:.0f}",
                f"使用字段: {page_view_field}"
            )

            # 断言验证结果
            assert result, f"总浏览量不一致: 分层={layered_pv}, 不分层={raw_pv} (字段: {page_view_field})"
        except Exception as e:
            self.record_result(case["id"], case["name"], False, "N/A", "N/A", str(e))
            assert False, f"测试失败: {str(e)}"

    def test_conversion_rate(self):
        """TC003: 下单转化率一致性验证（使用实际字段）"""
        case = next(c for c in TEST_CONFIG["test_cases"] if c["id"] == "TC003")
        visitor_field = TEST_CONFIG["field_mapping"]["total_visitors"]
        order_field = TEST_CONFIG["field_mapping"]["order_count"]

        try:
            # 计算分层数据的转化率
            layered_visitors = self.layered_metrics['terminal_metrics'][visitor_field].sum()
            layered_orders = self.layered_metrics['terminal_metrics'][order_field].sum()
            layered_conv = (layered_orders / layered_visitors) * 100 if layered_visitors > 0 else 0

            # 计算不分层数据的转化率
            raw_visitors = self.raw_metrics['terminal_metrics'][visitor_field].sum()
            raw_orders = self.raw_metrics['terminal_metrics'][order_field].sum()
            raw_conv = (raw_orders / raw_visitors) * 100 if raw_visitors > 0 else 0

            # 验证是否在容差范围内（相对容差5%）
            result = np.isclose(layered_conv, raw_conv, rtol=TEST_CONFIG["tolerance"]["conversion"])

            # 记录测试结果
            self.record_result(
                case["id"], case["name"], result,
                f"{layered_conv:.2f}%", f"{raw_conv:.2f}%",
                f"使用字段: {visitor_field}(访客), {order_field}(订单)"
            )

            # 断言验证结果
            assert result, f"转化率不一致: 分层={layered_conv:.2f}%, 不分层={raw_conv:.2f}%"
        except Exception as e:
            self.record_result(case["id"], case["name"], False, "N/A", "N/A", str(e))
            assert False, f"测试失败: {str(e)}"


# ------------------------------
# 执行测试
# ------------------------------
if __name__ == "__main__":
    # 执行pytest并生成HTML测试报告
    pytest.main([
        "-v", __file__,  # 详细输出模式
        f"--html={os.path.join(TEST_CONFIG['result_dir'], 'test_report.html')}",  # HTML报告路径
        "--self-contained-html"  # 报告包含所有资源，独立文件
    ])
