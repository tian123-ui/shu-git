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


class DWSLayerGenerator:
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

    def create_dws_tables(self):
        """创建DWS层表结构"""
        if not self.conn:
            raise Exception("请先建立数据库连接")

        table_sqls = [
            # 1. 页面访问指标汇总表
            """
            CREATE TABLE IF NOT EXISTS dws_page_visit_stats (
                stats_id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '统计ID',
                time_granularity ENUM('日','7天','月') NOT NULL COMMENT '时间粒度',
                page_type ENUM('店铺页','商品详情页','店铺其他页','外部页','功能页') NOT NULL COMMENT '页面类型',
                terminal_type ENUM('无线端','PC端') NOT NULL COMMENT '终端类型',
                visitor_count INT NOT NULL COMMENT '访客数',
                page_view INT NOT NULL COMMENT '浏览量',
                avg_stay_duration DECIMAL(10,2) NOT NULL COMMENT '平均停留时长(秒)',
                order_buyer_count INT NOT NULL COMMENT '下单买家数',
                dt DATE NOT NULL COMMENT '分区日期',
                UNIQUE KEY uk_stats (time_granularity, page_type, terminal_type, dt) COMMENT '唯一约束，防止重复数据'
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='页面访问指标汇总表';
            """,

            # 2. 店内路径流转汇总表
            """
            CREATE TABLE IF NOT EXISTS dws_instore_path_flow_stats (
                flow_id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '流转ID',
                time_granularity ENUM('日','7天','月') NOT NULL COMMENT '时间粒度',
                terminal_type ENUM('无线端','PC端') NOT NULL COMMENT '终端类型',
                source_page_id INT COMMENT '来源页面ID',
                target_page_id INT NOT NULL COMMENT '去向页面ID',
                visitor_count INT NOT NULL COMMENT '访客数',
                dt DATE NOT null comment '分区日期',
                UNIQUE KEY uk_flow (time_granularity, terminal_type, source_page_id, target_page_id, dt) COMMENT '唯一约束'
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='店内内路径流转汇总表';
            """,

            # 3. PC端流量入口汇总表
            """
            CREATE TABLE IF NOT EXISTS dws_pc_traffic_entry_stats (
                id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID',
                time_granularity ENUM('日','7天','月') NOT NULL COMMENT '时间粒度',
                source_page_id INT COMMENT '来源页面ID',
                source_page_name VARCHAR(100) COMMENT '来源页面名称',
                visitor_count INT NOT NULL COMMENT '访客数',
                visitor_ratio DECIMAL(10,4) NOT NULL COMMENT '访客占比',
                dt DATE NOT NULL COMMENT '分区日期',
                UNIQUE KEY uk_pc_entry (time_granularity, source_page_id, dt) COMMENT '唯一约束'
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='PC端流量入口汇总表';
            """
        ]

        try:
            with self.conn.cursor() as cursor:
                for sql in table_sqls:
                    cursor.execute(sql)
            self.conn.commit()
            print("DWS层表结构创建完成")
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

    def check_dwd_data(self, start_date, end_date):
        """检查DWD层是否有指定日期范围的数据"""
        if not self.conn:
            raise Exception("请先建立数据库连接")

        with self.conn.cursor() as cursor:
            cursor.execute(f"""
            SELECT COUNT(*) as count FROM dwd_user_page_visit_detail 
            WHERE dt BETWEEN '{start_date}' AND '{end_date}'
            """)
            result = cursor.fetchone()
            return result['count'] > 0

    def generate_dws_data(self, start_date, end_date):
        """生成DWS层所有表数据"""
        if not self.check_dwd_data(start_date, end_date):
            raise Exception(f"DWD层在 {start_date} 至 {end_date} 期间无数据，请先确保DWD层数据生成成功")

        with self.conn.cursor() as cursor:
            # 1. 生成页面访问指标汇总表数据
            print("开始生成dws_page_visit_stats数据...")
            cursor.execute(f"""
            SELECT 
                '日' as time_granularity,
                page_type,
                terminal_type,
                COUNT(DISTINCT visitor_id) as visitor_count,
                COUNT(*) as page_view,
                AVG(stay_duration) as avg_stay_duration,
                SUM(is_order) as order_buyer_count,
                dt
            FROM (
                SELECT 
                    d.*,
                    (SELECT terminal_type FROM dim_device WHERE device_id = d.device_id) as terminal_type
                FROM dwd_user_page_visit_detail d
                WHERE dt BETWEEN '{start_date}' AND '{end_date}'
            ) t
            GROUP BY page_type, terminal_type, dt
            """)
            page_stats_data = cursor.fetchall()
            self._batch_insert_ignore("dws_page_visit_stats", page_stats_data)

            # 2. 生成店内路径流转汇总表数据
            print("开始生成dws_instore_path_flow_stats数据...")
            cursor.execute(f"""
            SELECT 
                '日' as time_granularity,
                terminal_type,
                source_page_id,
                target_page_id,
                COUNT(DISTINCT visitor_id) as visitor_count,
                dt
            FROM (
                SELECT 
                    d.*,
                    (SELECT terminal_type FROM dim_device WHERE device_id = d.device_id) as terminal_type
                FROM dwd_user_page_visit_detail d
                WHERE dt BETWEEN '{start_date}' AND '{end_date}'
                  AND target_page_id IS NOT NULL
            ) t
            GROUP BY terminal_type, source_page_id, target_page_id, dt
            """)
            path_flow_data = cursor.fetchall()
            self._batch_insert_ignore("dws_instore_path_flow_stats", path_flow_data)

            # 3. 生成PC端流量入口汇总表数据
            print("开始生成dws_pc_traffic_entry_stats数据...")
            cursor.execute(f"""
            SELECT 
                '日' as time_granularity,
                d.source_page_id,
                p.page_name as source_page_name,
                COUNT(DISTINCT d.visitor_id) as visitor_count,
                COUNT(DISTINCT d.visitor_id) / total.total_visitors as visitor_ratio,
                d.dt
            FROM dwd_user_page_visit_detail d
            LEFT JOIN dim_page p ON d.source_page_id = p.page_id
            CROSS JOIN (
                SELECT COUNT(DISTINCT visitor_id) as total_visitors, dt 
                FROM dwd_user_page_visit_detail 
                WHERE device_id IN (SELECT device_id FROM dim_device WHERE terminal_type = 'PC端')
                  AND dt BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY dt
            ) total ON d.dt = total.dt
            WHERE d.device_id IN (SELECT device_id FROM dim_device WHERE terminal_type = 'PC端')
              AND d.dt BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY d.source_page_id, p.page_name, d.dt, total.total_visitors
            """)
            pc_entry_data = cursor.fetchall()
            self._batch_insert_ignore("dws_pc_traffic_entry_stats", pc_entry_data)

        print("DWS层所有表数据生成完成")

    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            print("数据库连接已关闭")


def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='生成DWS层汇总数据')
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

    generator = DWSLayerGenerator()
    try:
        # 连接数据库
        if not generator.connect():
            return

        # 创建表结构
        generator.create_dws_tables()

        # 生成DWS层数据
        print(f"开始基于DWD层数据生成DWS层数据（{args.start_date}至{args.end_date}）...")
        generator.generate_dws_data(args.start_date, args.end_date)

        print("DWS层数据生成完成！")

    except Exception as e:
        print(f"执行出错：{str(e)}")
    finally:
        generator.close()


if __name__ == "__main__":
    main()
