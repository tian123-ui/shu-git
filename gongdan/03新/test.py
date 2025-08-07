import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, timedelta
import unittest


# 生成测试数据
def generate_test_data():
    """生成模拟的各层数据表数据用于测试"""
    # 设置随机种子，确保结果可复现
    np.random.seed(42)

    # 生成日期范围
    dates = [datetime.now() - timedelta(days=i) for i in range(30)]
    date_strings = [d.strftime('%Y-%m-%d') for d in dates]

    # 1. ODS层数据
    # 销售原始数据
    ods_sales = pd.DataFrame({
        '订单时间': np.random.choice(date_strings, 1000),
        '商品ID': np.random.choice(['P001', 'P002', 'P003', 'P004', 'P005'], 1000),
        'SKU ID': np.random.choice(['S001', 'S002', 'S003', 'S004', 'S005', 'S006', 'S007', 'S008', 'S009', 'S010'],
                                   1000),
        '销售数量': np.random.randint(1, 10, 1000),
        '单价': np.random.uniform(10, 1000, 1000),
        '订单金额': np.random.uniform(10, 10000, 1000),
        '购买用户ID': np.random.choice(['U001', 'U002', 'U003', 'U004', 'U005', 'U006', 'U007', 'U008', 'U009', 'U010'],
                                       1000)
    })
    ods_sales['订单金额'] = ods_sales['销售数量'] * ods_sales['单价']  # 确保金额正确

    # 流量原始数据
    ods_traffic = pd.DataFrame({
        '访问时间': np.random.choice(date_strings, 5000),
        '平台': np.random.choice(['iOS', 'Android', 'Web'], 5000),
        '渠道': np.random.choice(['App Store', '搜索引擎', '社交媒体', '直接访问'], 5000),
        '访客ID': np.random.choice(['V001', 'V002', 'V003', 'V004', 'V005'] + [f'V{i:03d}' for i in range(6, 100)],
                                   5000),
        '商品ID': np.random.choice(['P001', 'P002', 'P003', 'P004', 'P005', None], 5000),
        '点击行为': np.random.choice([0, 1], 5000, p=[0.3, 0.7]),
        '转化行为': np.random.choice([0, 1], 5000, p=[0.8, 0.2])
    })

    # 用户行为原始数据
    ods_user_behavior = pd.DataFrame({
        '用户ID': np.random.choice(['U001', 'U002', 'U003', 'U004', 'U005', 'U006', 'U007', 'U008', 'U009', 'U010'],
                                   1000),
        '商品ID': np.random.choice(['P001', 'P002', 'P003', 'P004', 'P005'], 1000),
        '消费时间': np.random.choice(date_strings, 1000),
        '消费金额': np.random.uniform(10, 10000, 1000),
        '消费次数': np.random.randint(1, 10, 1000)
    })

    # 2. DIM层数据 - 商品SKU维度表
    dim_product_sku = pd.DataFrame({
        '商品ID': ['P001', 'P001', 'P002', 'P002', 'P003', 'P004', 'P004', 'P005'],
        'SKU ID': ['S001', 'S002', 'S003', 'S004', 'S005', 'S006', 'S007', 'S008'],
        '商品名称': ['商品A', '商品A', '商品B', '商品B', '商品C', '商品D', '商品D', '商品E'],
        '所属类目': ['类目1', '类目1', '类目2', '类目2', '类目1', '类目3', '类目3', '类目2']
    })

    # 3. DWD层数据 - SKU销售明细表
    dwd_sku_sales = ods_sales.groupby(['订单时间', '商品ID', 'SKU ID']).agg({
        '销售数量': 'sum',
        '订单金额': 'sum'
    }).reset_index()
    dwd_sku_sales.rename(columns={'订单时间': '日期'}, inplace=True)

    # 4. DWS层数据 - 销售汇总表
    dws_sales_summary = dwd_sku_sales.groupby(['日期', '商品ID']).agg({
        '销售数量': 'sum',
        '订单金额': 'sum'
    }).reset_index()
    dws_sales_summary.rename(columns={'销售数量': '总销量', '订单金额': '总营收'}, inplace=True)

    # 5. ADS层数据 - 商品核心概览表
    ads_product_core = dws_sales_summary.copy()

    return {
        'ods_sales': ods_sales,
        'ods_traffic': ods_traffic,
        'ods_user_behavior': ods_user_behavior,
        'dim_product_sku': dim_product_sku,
        'dwd_sku_sales': dwd_sku_sales,
        'dws_sales_summary': dws_sales_summary,
        'ads_product_core': ads_product_core
    }


# 创建测试数据库
def create_test_database(data):
    """创建SQLite数据库并插入测试数据"""
    conn = sqlite3.connect('product_360_test.db')

    # 将所有数据表写入数据库
    for table_name, df in data.items():
        df.to_sql(table_name, conn, if_exists='replace', index=False)

    conn.close()
    return 'product_360_test.db'


# 实现分层和不分层计算函数
def calculate_total_sales_layered(db_path, product_id=None, start_date=None, end_date=None):
    """分层方式计算总销量：从DWS层汇总数据计算"""
    conn = sqlite3.connect(db_path)

    query = "SELECT SUM(总销量) as total_sales FROM dws_sales_summary"
    params = []

    # 添加筛选条件
    where_clause = []
    if product_id:
        where_clause.append("商品ID = ?")
        params.append(product_id)
    if start_date and end_date:
        where_clause.append("日期 BETWEEN ? AND ?")
        params.extend([start_date, end_date])

    if where_clause:
        query += " WHERE " + " AND ".join(where_clause)

    result = pd.read_sql(query, conn, params=params)
    conn.close()

    return result['total_sales'].iloc[0] or 0


def calculate_total_sales_non_layered(db_path, product_id=None, start_date=None, end_date=None):
    """不分层方式计算总销量：从ODS层原始数据直接计算"""
    conn = sqlite3.connect(db_path)

    query = "SELECT SUM(销售数量) as total_sales FROM ods_sales"
    params = []

    # 添加筛选条件
    where_clause = []
    if product_id:
        where_clause.append("商品ID = ?")
        params.append(product_id)
    if start_date and end_date:
        where_clause.append("订单时间 BETWEEN ? AND ?")
        params.extend([start_date, end_date])

    if where_clause:
        query += " WHERE " + " AND ".join(where_clause)

    result = pd.read_sql(query, conn, params=params)
    conn.close()

    return result['total_sales'].iloc[0] or 0


def calculate_total_revenue_layered(db_path, product_id=None, start_date=None, end_date=None):
    """分层方式计算总营收：从DWS层汇总数据计算"""
    conn = sqlite3.connect(db_path)

    query = "SELECT SUM(总营收) as total_revenue FROM dws_sales_summary"
    params = []

    # 添加筛选条件
    where_clause = []
    if product_id:
        where_clause.append("商品ID = ?")
        params.append(product_id)
    if start_date and end_date:
        where_clause.append("日期 BETWEEN ? AND ?")
        params.extend([start_date, end_date])

    if where_clause:
        query += " WHERE " + " AND ".join(where_clause)

    result = pd.read_sql(query, conn, params=params)
    conn.close()

    return round(result['total_revenue'].iloc[0] or 0, 2)


def calculate_total_revenue_non_layered(db_path, product_id=None, start_date=None, end_date=None):
    """不分层方式计算总营收：从ODS层原始数据直接计算"""
    conn = sqlite3.connect(db_path)

    query = "SELECT SUM(订单金额) as total_revenue FROM ods_sales"
    params = []

    # 添加筛选条件
    where_clause = []
    if product_id:
        where_clause.append("商品ID = ?")
        params.append(product_id)
    if start_date and end_date:
        where_clause.append("订单时间 BETWEEN ? AND ?")
        params.extend([start_date, end_date])

    if where_clause:
        query += " WHERE " + " AND ".join(where_clause)

    result = pd.read_sql(query, conn, params=params)
    conn.close()

    return round(result['total_revenue'].iloc[0] or 0, 2)


def calculate_uv_layered(db_path, product_id=None, start_date=None, end_date=None):
    """分层方式计算访客数：从DWD层数据计算"""
    # 注意：实际项目中会有专门的流量汇总表，这里简化处理
    conn = sqlite3.connect(db_path)

    query = """
    SELECT COUNT(DISTINCT 访客ID) as uv 
    FROM ods_traffic
    WHERE 商品ID IS NOT NULL
    """
    params = []

    # 添加筛选条件
    if product_id:
        query += " AND 商品ID = ?"
        params.append(product_id)
    if start_date and end_date:
        query += " AND 访问时间 BETWEEN ? AND ?"
        params.extend([start_date, end_date])

    result = pd.read_sql(query, conn, params=params)
    conn.close()

    return result['uv'].iloc[0] or 0


def calculate_uv_non_layered(db_path, product_id=None, start_date=None, end_date=None):
    """不分层方式计算访客数：从ODS层原始数据直接计算"""
    conn = sqlite3.connect(db_path)

    query = """
    SELECT COUNT(DISTINCT 访客ID) as uv 
    FROM ods_traffic
    WHERE 商品ID IS NOT NULL
    """
    params = []

    # 添加筛选条件
    if product_id:
        query += " AND 商品ID = ?"
        params.append(product_id)
    if start_date and end_date:
        query += " AND 访问时间 BETWEEN ? AND ?"
        params.extend([start_date, end_date])

    result = pd.read_sql(query, conn, params=params)
    conn.close()

    return result['uv'].iloc[0] or 0


# 测试用例
class TestProduct360Dashboard(unittest.TestCase):
    """商品360看板测试用例"""

    @classmethod
    def setUpClass(cls):
        """测试前准备：生成测试数据和数据库"""
        print("正在生成测试数据...")
        cls.test_data = generate_test_data()
        cls.db_path = create_test_database(cls.test_data)

        # 获取日期范围
        dates = sorted(cls.test_data['ods_sales']['订单时间'].unique())
        cls.start_date = dates[0]
        cls.mid_date = dates[len(dates) // 2]
        cls.end_date = dates[-1]

        # 获取商品列表
        cls.product_ids = cls.test_data['ods_sales']['商品ID'].unique()
        print(f"测试准备完成，日期范围: {cls.start_date} 至 {cls.end_date}")
        print(f"测试商品列表: {', '.join(cls.product_ids)}")

    def test_total_sales_all(self):
        """测试总销量 - 全量数据"""
        layered = calculate_total_sales_layered(self.db_path)
        non_layered = calculate_total_sales_non_layered(self.db_path)
        self.assertEqual(layered, non_layered,
                         f"总销量全量数据不一致: 分层={layered}, 不分层={non_layered}")

    def test_total_sales_by_product(self):
        """测试总销量 - 按商品筛选"""
        for product_id in self.product_ids:
            with self.subTest(product_id=product_id):
                layered = calculate_total_sales_layered(self.db_path, product_id=product_id)
                non_layered = calculate_total_sales_non_layered(self.db_path, product_id=product_id)
                self.assertEqual(layered, non_layered,
                                 f"商品 {product_id} 总销量不一致: 分层={layered}, 不分层={non_layered}")

    def test_total_sales_by_date_range(self):
        """测试总销量 - 按日期范围筛选"""
        layered = calculate_total_sales_layered(
            self.db_path, start_date=self.start_date, end_date=self.mid_date)
        non_layered = calculate_total_sales_non_layered(
            self.db_path, start_date=self.start_date, end_date=self.mid_date)
        self.assertEqual(layered, non_layered,
                         f"日期范围内总销量不一致: 分层={layered}, 不分层={non_layered}")

    def test_total_revenue_all(self):
        """测试总营收 - 全量数据"""
        layered = calculate_total_revenue_layered(self.db_path)
        non_layered = calculate_total_revenue_non_layered(self.db_path)
        self.assertEqual(layered, non_layered,
                         f"总营收全量数据不一致: 分层={layered}, 不分层={non_layered}")

    def test_total_revenue_by_product(self):
        """测试总营收 - 按商品筛选"""
        for product_id in self.product_ids:
            with self.subTest(product_id=product_id):
                layered = calculate_total_revenue_layered(self.db_path, product_id=product_id)
                non_layered = calculate_total_revenue_non_layered(self.db_path, product_id=product_id)
                self.assertEqual(layered, non_layered,
                                 f"商品 {product_id} 总营收不一致: 分层={layered}, 不分层={non_layered}")

    def test_uv_all(self):
        """测试访客数 - 全量数据"""
        layered = calculate_uv_layered(self.db_path)
        non_layered = calculate_uv_non_layered(self.db_path)
        self.assertEqual(layered, non_layered,
                         f"访客数全量数据不一致: 分层={layered}, 不分层={non_layered}")

    def test_uv_by_product(self):
        """测试访客数 - 按商品筛选"""
        for product_id in self.product_ids:
            with self.subTest(product_id=product_id):
                layered = calculate_uv_layered(self.db_path, product_id=product_id)
                non_layered = calculate_uv_non_layered(self.db_path, product_id=product_id)
                self.assertEqual(layered, non_layered,
                                 f"商品 {product_id} 访客数不一致: 分层={layered}, 不分层={non_layered}")


if __name__ == '__main__':
    # 运行测试并生成测试报告
    print("===== 商品360看板数据一致性测试 =====")
    test_suite = unittest.TestLoader().loadTestsFromTestCase(TestProduct360Dashboard)
    test_result = unittest.TextTestRunner(verbosity=2).run(test_suite)

    # 输出测试总结
    print("\n===== 测试总结 =====")
    print(f"测试用例总数: {test_result.testsRun}")
    print(f"通过: {test_result.testsRun - len(test_result.failures) - len(test_result.errors)}")
    print(f"失败: {len(test_result.failures)}")
    print(f"错误: {len(test_result.errors)}")

    if len(test_result.failures) == 0 and len(test_result.errors) == 0:
        print("测试通过: 分层与不分层实现方式的数据一致")
    else:
        print("测试未通过: 存在数据不一致的情况")
