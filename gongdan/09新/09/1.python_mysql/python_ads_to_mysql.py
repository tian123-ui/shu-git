import pymysql
from pymysql import OperationalError, ProgrammingError
from datetime import datetime
import argparse

# MySQL连接配置
CONFIG = {
    'host': 'cdh03',
    'user': 'root',
    'password': 'root',
    'database': 'gon09',
    'port': 3306,
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
    'autocommit': False
}


class ADSLayerGenerator:
    def __init__(self):
        self.conn = None

    def connect(self):
        """建立数据库连接"""
        try:
            self.conn = pymysql.connect(** CONFIG)
            print(f"成功连接到数据库 {CONFIG['database']}")
            return True
        except OperationalError as e:
            print(f"数据库连接错误: {str(e)}")
            return False

    def create_ads_tables(self):
        """创建ADS层表结构"""
        if not self.conn:
            raise Exception("请先建立数据库连接")

        table_sqls = [
            # 1. 无线端入店与承接指标表
            """
            CREATE TABLE IF NOT EXISTS ads_wireless_entry_receive (
                id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID',
                time_dimension ENUM('日','7天','月') NOT NULL COMMENT '时间维度',
                entry_page_type ENUM('店铺页','商品详情页','店铺其他页','外部页','功能页') NOT NULL COMMENT '入店页面类型',
                visitor_count INT NOT NULL COMMENT '访客数',
                order_buyer_count INT NOT NULL COMMENT '下单买家数',
                conversion_rate DECIMAL(10,4) GENERATED ALWAYS AS 
                    (IF(visitor_count=0, 0, order_buyer_count/visitor_count)) STORED COMMENT '转化率=下单买家数/访客数',
                dt DATE NOT NULL COMMENT '分区日期',
                UNIQUE KEY uk_wireless (time_dimension, entry_page_type, dt) COMMENT '唯一约束'
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='无线端入店与承接指标表';
            """,

            # 2. 页面访问排行表
            """
            CREATE TABLE IF NOT EXISTS ads_page_visit_rank (
                id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID',
                time_dimension ENUM('日','7天','月') NOT NULL COMMENT '时间维度',
                terminal_type ENUM('无线端','PC端') NOT NULL COMMENT '终端类型',
                page_type ENUM('店铺页','商品详情页','店铺其他页','外部页','功能页') NOT NULL COMMENT '页面类型',
                page_name VARCHAR(100) NOT NULL COMMENT '页面名称',
                visitor_count INT NOT NULL COMMENT '访客数',
                rank INT NOT NULL COMMENT '排名',
                dt DATE NOT NULL COMMENT '分区日期',
                UNIQUE KEY uk_rank (time_dimension, terminal_type, page_type, dt, rank) COMMENT '唯一约束'
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='页面访问排行表';
            """,

            # 3. 店内路径展示表
            """
            CREATE TABLE IF NOT EXISTS ads_instore_path_display (
                id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID',
                time_dimension ENUM('日','7天','月') NOT NULL COMMENT '时间维度',
                terminal_type ENUM('无线端','PC端') NOT NULL COMMENT '终端类型',
                source_page_name VARCHAR(100) COMMENT '来源页面名称',
                target_page_name VARCHAR(100) NOT NULL COMMENT '去向页面名称',
                visitor_count INT NOT NULL COMMENT '访客数',
                proportion DECIMAL(10,4) NOT NULL COMMENT '占比',
                dt DATE NOT NULL COMMENT '分区日期',
                UNIQUE KEY uk_path (time_dimension, terminal_type, source_page_name, target_page_name, dt) COMMENT '唯一约束'
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='店内路径展示表';
            """,

            # 4. PC端流量入口TOP20表
            """
            CREATE TABLE IF NOT EXISTS ads_pc_entry_top20 (
                id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID',
                time_dimension ENUM('日','7天','月') NOT NULL COMMENT '时间维度',
                source_page_name VARCHAR(100) NOT NULL COMMENT '来源页面名称',
                visitor_count INT NOT NULL COMMENT '访客数',
                visitor_ratio DECIMAL(10,4) NOT NULL COMMENT '访客占比',
                rank INT NOT NULL COMMENT '排名',
                dt DATE NOT NULL COMMENT '分区日期',
                UNIQUE KEY uk_pc_top20 (time_dimension, source_page_name, dt) COMMENT '唯一约束'
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='PC端流量入口TOP20表';
            """
        ]

        try:
            with self.conn.cursor() as cursor:
                for sql in table_sqls:
                    cursor.execute(sql)
            self.conn.commit()
            print("ADS层表结构创建完成")
        except Exception as e:
            self.conn.rollback()
            print(f"创建表结构失败: {str(e)}")
            raise

    def _batch_insert_ignore(self, table_name, data_list, batch_size=1000):
        """批量插入并忽略重复数据"""
        if not data_list:
            print(f"{table_name} 无数据可插入")
            return

        columns = list(data_list[0].keys())
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = f"INSERT IGNORE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        try:
            with self.conn.cursor() as cursor:
                total = 0
                for i in range(0, len(data_list), batch_size):
                    batch = data_list[i:i + batch_size]
                    values = [tuple(item[col] for col in columns) for item in batch]
                    cursor.executemany(insert_sql, values)
                    self.conn.commit()
                    total += len(batch)
                print(f"{table_name} 数据插入完成，共处理 {total} 条")
        except Exception as e:
            self.conn.rollback()
            print(f"插入数据失败: {str(e)}")
            raise

    def check_dws_data(self, start_date, end_date):
        """检查DWS层是否有指定日期范围的数据"""
        if not self.conn:
            raise Exception("请先建立数据库连接")

        with self.conn.cursor() as cursor:
            cursor.execute(f"""
            SELECT COUNT(*) as count FROM dws_page_visit_stats 
            WHERE dt BETWEEN '{start_date}' AND '{end_date}'
            """)
            result = cursor.fetchone()
            return result['count'] > 0

    def generate_ads_data(self, start_date, end_date):
        """生成ADS层所有表数据"""
        if not self.check_dws_data(start_date, end_date):
            raise Exception(f"DWS层在 {start_date} 至 {end_date} 期间无数据，请先确保DWS层数据生成成功")

        with self.conn.cursor() as cursor:
            # 1. 生成无线端入店与承接指标表数据
            print("开始生成ads_wireless_entry_receive数据...")
            cursor.execute(f"""
            SELECT 
                '日' as time_dimension,
                page_type as entry_page_type,
                SUM(visitor_count) as visitor_count,
                SUM(order_buyer_count) as order_buyer_count,
                dt
            FROM dws_page_visit_stats
            WHERE terminal_type = '无线端'
              AND dt BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY page_type, dt
            """)
            wireless_data = cursor.fetchall()
            self._batch_insert_ignore("ads_wireless_entry_receive", wireless_data)

            # 2. 生成页面访问排行表数据
            print("开始生成ads_page_visit_rank数据...")
            cursor.execute(f"""
            SELECT 
                time_granularity as time_dimension,
                terminal_type,
                page_type,
                page_name,
                visitor_count,
                rank,
                dt
            FROM (
                SELECT 
                    s.time_granularity,
                    s.terminal_type,
                    s.page_type,
                    p.page_name,
                    s.visitor_count,
                    @rank := IF(
                        @prev_group = CONCAT(s.terminal_type, s.page_type, s.time_granularity, s.dt),
                        @rank + 1,
                        1
                    ) as rank,
                    @prev_group := CONCAT(s.terminal_type, s.page_type, s.time_granularity, s.dt),
                    s.dt
                FROM dws_page_visit_stats s
                JOIN dim_page p ON s.page_type = p.page_type
                CROSS JOIN (SELECT @rank := 0, @prev_group := '') r
                WHERE s.dt BETWEEN '{start_date}' AND '{end_date}'
                ORDER BY s.time_granularity, s.terminal_type, s.page_type, s.visitor_count DESC, s.dt
            ) t
            """)
            rank_data = cursor.fetchall()
            self._batch_insert_ignore("ads_page_visit_rank", rank_data)

            # 3. 生成店内路径展示表数据
            print("开始生成ads_instore_path_display数据...")
            cursor.execute(f"""
            SELECT 
                '日' as time_dimension,
                s.terminal_type,
                p1.page_name as source_page_name,
                p2.page_name as target_page_name,
                s.visitor_count,
                s.visitor_count / total.total_visitors as proportion,
                s.dt
            FROM dws_instore_path_flow_stats s
            LEFT JOIN dim_page p1 ON s.source_page_id = p1.page_id
            LEFT JOIN dim_page p2 ON s.target_page_id = p2.page_id
            CROSS JOIN (
                SELECT SUM(visitor_count) as total_visitors, terminal_type, dt 
                FROM dws_instore_path_flow_stats
                WHERE dt BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY terminal_type, dt
            ) total ON s.terminal_type = total.terminal_type AND s.dt = total.dt
            WHERE s.dt BETWEEN '{start_date}' AND '{end_date}'
            """)
            path_display_data = cursor.fetchall()
            self._batch_insert_ignore("ads_instore_path_display", path_display_data)

            # 4. 生成PC端流量入口TOP20表数据
            print("开始生成ads_pc_entry_top20数据...")
            cursor.execute(f"""
            SELECT 
                time_granularity as time_dimension,
                source_page_name,
                visitor_count,
                visitor_ratio,
                rank,
                dt
            FROM (
                SELECT 
                    s.time_granularity,
                    s.source_page_name,
                    s.visitor_count,
                    s.visitor_ratio,
                    @rank := IF(@prev_dt = CONCAT(s.time_granularity, s.dt), @rank + 1, 1) as rank,
                    @prev_dt := CONCAT(s.time_granularity, s.dt),
                    s.dt
                FROM dws_pc_traffic_entry_stats s
                CROSS JOIN (SELECT @rank := 0, @prev_dt := '') r
                WHERE s.dt BETWEEN '{start_date}' AND '{end_date}'
                ORDER BY s.time_granularity, s.dt, s.visitor_count DESC
            ) t
            WHERE rank <= 20  # 只取前20名
            """)
            pc_top20_data = cursor.fetchall()
            self._batch_insert_ignore("ads_pc_entry_top20", pc_top20_data)

        print("ADS层所有表数据生成完成")

    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            print("数据库连接已关闭")


def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='生成ADS层数据')
    parser.add_argument('--start-date', default='2025-01-01', help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', default='2025-01-31', help='结束日期 (YYYY-MM-DD)')
    args = parser.parse_args()

    # 验证日期格式
    try:
        datetime.strptime(args.start_date, "%Y-%m-%d")
        datetime.strptime(args.end_date, "%Y-%m-%d")
    except ValueError:
        print("日期格式错误，请使用YYYY-MM-DD格式")
        return

    generator = ADSLayerGenerator()
    try:
        # 连接数据库
        if not generator.connect():
            return

        # 创建表结构
        generator.create_ads_tables()

        # 生成ADS层数据
        print(f"开始基于DWS层数据生成ADS层数据（{args.start_date}至{args.end_date}）...")
        generator.generate_ads_data(args.start_date, args.end_date)

        print("ADS层数据生成完成！")

    except Exception as e:
        print(f"执行出错：{str(e)}")
    finally:
        generator.close()


if __name__ == "__main__":
    main()
