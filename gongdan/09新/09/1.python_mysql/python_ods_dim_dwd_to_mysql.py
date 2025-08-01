import pymysql
from pymysql import OperationalError, IntegrityError
from datetime import datetime, timedelta
import random
import uuid
import argparse


class MySQLDataGenerator:
    def __init__(self, host='cdh03', port=3306, user='root', password='root', db='gon09'):
        self.config = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': db,
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor,
            'autocommit': False  # 手动控制事务
        }
        self.conn = None

    def connect(self):
        """建立数据库连接，自动创建数据库"""
        try:
            # 尝试直接连接目标数据库
            self.conn = pymysql.connect(**self.config)
            print(f"成功连接到数据库 {self.config['database']}")
            return True
        except OperationalError as e:
            if e.args[0] == 1049:  # 数据库不存在
                print(f"数据库 {self.config['database']} 不存在，正在创建...")
                # 先连接到MySQL服务器（不指定数据库）
                temp_conn = pymysql.connect(
                    host=self.config['host'],
                    port=self.config['port'],
                    user=self.config['user'],
                    password=self.config['password'],
                    charset=self.config['charset']
                )
                with temp_conn.cursor() as cursor:
                    cursor.execute(f"CREATE DATABASE {self.config['database']} CHARACTER SET utf8mb4")
                temp_conn.close()
                # 重新连接到新创建的数据库
                self.conn = pymysql.connect(**self.config)
                print(f"数据库 {self.config['database']} 创建并连接成功")
                return True
            else:
                print(f"连接失败: {str(e)}")
                return False

    def create_tables(self):
        """创建ODS、DIM、DWD层表结构"""
        if not self.conn:
            raise Exception("未建立数据库连接，请先调用connect()")

        table_definitions = [
            # ODS层：用户行为日志表
            """
            CREATE TABLE IF NOT EXISTS ods_page_behavior (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                visitor_id VARCHAR(50) NOT NULL COMMENT '访客唯一标识',
                visit_time DATETIME NOT NULL COMMENT '访问时间',
                page_name VARCHAR(100) NOT NULL COMMENT '页面名称',
                page_type ENUM('店铺页','商品详情页','店铺其他页','外部页','功能页') NOT NULL COMMENT '页面类型',
                source_page VARCHAR(100) COMMENT '来源页面',
                target_page VARCHAR(100) COMMENT '去向页面',
                stay_duration INT NOT NULL COMMENT '停留时长(秒)',
                terminal_type ENUM('无线端','PC端') NOT NULL COMMENT '终端类型',
                is_order TINYINT NOT NULL DEFAULT 0 COMMENT '是否下单(1=是)',
                order_buyer_id VARCHAR(50) COMMENT '下单用户ID',
                dt DATE NOT NULL COMMENT '分区日期',
                INDEX idx_dt (dt),
                INDEX idx_visitor (visitor_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户行为原始日志表';
            """,

            # DIM层：页面维度表
            """
            CREATE TABLE IF NOT EXISTS dim_page (
                page_id INT PRIMARY KEY COMMENT '页面ID',
                page_type ENUM('店铺页','商品详情页','店铺其他页','外部页','功能页') NOT NULL COMMENT '页面类型',
                page_subtype VARCHAR(50) NOT NULL COMMENT '页面子类型',
                page_name VARCHAR(100) NOT NULL UNIQUE COMMENT '页面名称',
                is_valid TINYINT DEFAULT 1 COMMENT '是否有效(1=有效)'
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='页面维度表';
            """,

            # DIM层：时间维度表
            """
            CREATE TABLE IF NOT EXISTS dim_time (
                time_id INT PRIMARY KEY COMMENT '时间ID',
                time_granularity ENUM('日','7天','月') NOT NULL COMMENT '时间粒度',
                date_code VARCHAR(20) NOT NULL COMMENT '日期编码',
                start_date DATE NOT NULL COMMENT '开始日期',
                end_date DATE NOT NULL COMMENT '结束日期',
                UNIQUE KEY uk_granularity_code (time_granularity, date_code)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='时间维度表';
            """,

            # DIM层：设备维度表
            """
            CREATE TABLE IF NOT EXISTS dim_device (
                device_id INT PRIMARY KEY COMMENT '设备ID',
                terminal_type ENUM('无线端','PC端') NOT NULL COMMENT '终端类型',
                device_identifier VARCHAR(50) NOT NULL COMMENT '设备标识',
                device_brand VARCHAR(50) COMMENT '设备品牌'
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='设备维度表';
            """,

            # DWD层：用户页面访问明细表
            """
            CREATE TABLE IF NOT EXISTS dwd_user_page_visit_detail (
                dwd_id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '明细ID',
                visitor_id VARCHAR(50) NOT NULL COMMENT '访客ID',
                time_id INT NOT NULL COMMENT '时间ID',
                page_id INT NOT NULL COMMENT '页面ID',
                page_type ENUM('店铺页','商品详情页','店铺其他页','外部页','功能页') NOT NULL COMMENT '页面类型',
                source_page_id INT COMMENT '来源页面ID',
                target_page_id INT COMMENT '去向页面ID',
                stay_duration INT NOT NULL COMMENT '停留时长(秒)',
                device_id INT NOT NULL COMMENT '设备ID',
                is_order TINYINT NOT NULL COMMENT '是否下单',
                order_buyer_id VARCHAR(50) COMMENT '下单用户ID',
                dt DATE NOT NULL COMMENT '分区日期',
                INDEX idx_time_page (time_id, page_id),
                INDEX idx_visitor (visitor_id),
                FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
                FOREIGN KEY (page_id) REFERENCES dim_page(page_id),
                FOREIGN KEY (device_id) REFERENCES dim_device(device_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户页面访问明细事实表';
            """
        ]

        try:
            with self.conn.cursor() as cursor:
                for sql in table_definitions:
                    cursor.execute(sql)
            self.conn.commit()
            print("所有表结构创建/验证完成")
        except Exception as e:
            self.conn.rollback()
            print(f"创建表结构失败: {str(e)}")
            raise

    def _batch_insert(self, table_name, data_list, batch_size=1000):
        """通用批量插入方法"""
        if not data_list:
            print(f"警告: {table_name} 没有数据可插入")
            return

        # 提取字段名
        columns = list(data_list[0].keys())
        placeholders = ", ".join(["%s"] * len(columns))
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        try:
            with self.conn.cursor() as cursor:
                total = 0
                # 分批插入
                for i in range(0, len(data_list), batch_size):
                    batch = data_list[i:i + batch_size]
                    # 转换为元组列表
                    values = [tuple(item[col] for col in columns) for item in batch]
                    cursor.executemany(sql, values)
                    self.conn.commit()
                    total += len(batch)
                    print(f"已插入 {total}/{len(data_list)} 条记录到 {table_name}")
        except IntegrityError as e:
            self.conn.rollback()
            print(f"数据完整性错误: {str(e)}")
            raise
        except Exception as e:
            self.conn.rollback()
            print(f"插入数据失败: {str(e)}")
            raise

    def generate_dim_data(self):
        """生成所有维度表数据"""
        # 页面维度数据
        dim_page = [
            {"page_id": 1, "page_type": "店铺页", "page_subtype": "首页", "page_name": "首页", "is_valid": 1},
            {"page_id": 2, "page_type": "店铺页", "page_subtype": "活动页", "page_name": "活动页", "is_valid": 1},
            {"page_id": 3, "page_type": "店铺页", "page_subtype": "分类页", "page_name": "分类页", "is_valid": 1},
            {"page_id": 4, "page_type": "店铺页", "page_subtype": "宝贝页", "page_name": "宝贝页", "is_valid": 1},
            {"page_id": 5, "page_type": "店铺页", "page_subtype": "新品页", "page_name": "新品页", "is_valid": 1},
            {"page_id": 6, "page_type": "商品详情页", "page_subtype": "基础详情页", "page_name": "商品详情页_1",
             "is_valid": 1},
            {"page_id": 7, "page_type": "商品详情页", "page_subtype": "基础详情页", "page_name": "商品详情页_2",
             "is_valid": 1},
            {"page_id": 8, "page_type": "商品详情页", "page_subtype": "基础详情页", "page_name": "商品详情页_3",
             "is_valid": 1},
            {"page_id": 9, "page_type": "店铺其他页", "page_subtype": "订阅页", "page_name": "订阅页", "is_valid": 1},
            {"page_id": 10, "page_type": "店铺其他页", "page_subtype": "直播页", "page_name": "直播页", "is_valid": 1},
            {"page_id": 11, "page_type": "外部页", "page_subtype": "搜索引擎", "page_name": "百度搜索", "is_valid": 1},
            {"page_id": 12, "page_type": "外部页", "page_subtype": "社交媒体", "page_name": "微信", "is_valid": 1},
            {"page_id": 13, "page_type": "功能页", "page_subtype": "购物流程", "page_name": "购物车", "is_valid": 1},
            {"page_id": 14, "page_type": "功能页", "page_subtype": "购物流程", "page_name": "下单页", "is_valid": 1}
        ]
        self._batch_insert("dim_page", dim_page)

        # 时间维度数据（2025年1月）
        dim_time = []
        time_id = 1
        # 日粒度
        for day in range(1, 32):
            date = datetime(2025, 1, day).date()
            dim_time.append({
                "time_id": time_id,
                "time_granularity": "日",
                "date_code": date.strftime("%Y%m%d"),
                "start_date": date,
                "end_date": date
            })
            time_id += 1
        # 7天粒度
        for week in range(4):
            start_day = week * 7 + 1
            end_day = min((week + 1) * 7, 31)
            dim_time.append({
                "time_id": time_id,
                "time_granularity": "7天",
                "date_code": f"202501W{week + 1}",
                "start_date": datetime(2025, 1, start_day).date(),
                "end_date": datetime(2025, 1, end_day).date()
            })
            time_id += 1
        # 月粒度
        dim_time.append({
            "time_id": time_id,
            "time_granularity": "月",
            "date_code": "202501",
            "start_date": datetime(2025, 1, 1).date(),
            "end_date": datetime(2025, 1, 31).date()
        })
        self._batch_insert("dim_time", dim_time)

        # 设备维度数据
        dim_device = [
            {"device_id": 1, "terminal_type": "无线端", "device_identifier": "iOS", "device_brand": "Apple"},
            {"device_id": 2, "terminal_type": "无线端", "device_identifier": "Android", "device_brand": "Huawei"},
            {"device_id": 3, "terminal_type": "无线端", "device_identifier": "Android", "device_brand": "Xiaomi"},
            {"device_id": 4, "terminal_type": "PC端", "device_identifier": "Windows", "device_brand": "Dell"},
            {"device_id": 5, "terminal_type": "PC端", "device_identifier": "macOS", "device_brand": "Apple"}
        ]
        self._batch_insert("dim_device", dim_device)

        print("DIM层所有维度数据生成完成")

    def generate_ods_data(self, start_date, end_date, record_count=10000):
        """生成ODS层用户行为数据"""
        # 先获取页面列表
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT page_name, page_type FROM dim_page")
            pages = cursor.fetchall()
            if not pages:
                raise Exception("DIM层页面数据为空，请先调用generate_dim_data()")

        # 解析日期
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        if start >= end:
            raise ValueError("开始日期必须早于结束日期")

        time_delta = end - start
        total_days = time_delta.days

        # 生成数据
        ods_data = []
        for _ in range(record_count):
            # 随机选择页面
            page = random.choice(pages)

            # 随机生成访问时间
            random_days = random.randint(0, total_days)
            visit_time = (start + timedelta(days=random_days)).replace(
                hour=random.randint(0, 23),
                minute=random.randint(0, 59),
                second=random.randint(0, 59)
            )

            # 来源页面（30%概率无）
            source_page = random.choice(pages)['page_name'] if random.random() > 0.3 else None

            # 去向页面（20%概率无）
            target_page = random.choice(pages)['page_name'] if random.random() > 0.2 else None

            # 终端类型（70%无线端）
            terminal_type = "无线端" if random.random() > 0.3 else "PC端"

            # 下单行为（5%概率）
            is_order = 1 if random.random() > 0.95 else 0
            order_buyer_id = f"BUYER_{uuid.uuid4().hex[:6].upper()}" if is_order else None

            ods_data.append({
                "visitor_id": f"VISITOR_{uuid.uuid4().hex[:8].upper()}",
                "visit_time": visit_time,
                "page_name": page['page_name'],
                "page_type": page['page_type'],
                "source_page": source_page,
                "target_page": target_page,
                "stay_duration": random.randint(10, 300),  # 10-300秒
                "terminal_type": terminal_type,
                "is_order": is_order,
                "order_buyer_id": order_buyer_id,
                "dt": visit_time.date()
            })

        self._batch_insert("ods_page_behavior", ods_data)
        print(f"ODS层数据生成完成，共 {record_count} 条")

    def generate_dwd_data(self, start_date, end_date):
        """基于ODS和DIM层生成DWD层数据"""
        # 获取维度映射关系
        with self.conn.cursor() as cursor:
            # 页面映射: page_name -> page_id
            cursor.execute("SELECT page_name, page_id FROM dim_page")
            page_map = {row['page_name']: row['page_id'] for row in cursor.fetchall()}

            # 时间映射: date_code -> time_id (日粒度)
            cursor.execute("SELECT date_code, time_id FROM dim_time WHERE time_granularity = '日'")
            time_map = {row['date_code']: row['time_id'] for row in cursor.fetchall()}

            # 设备映射: terminal_type -> [device_ids]
            cursor.execute("SELECT terminal_type, device_id FROM dim_device")
            device_map = {}
            for row in cursor.fetchall():
                if row['terminal_type'] not in device_map:
                    device_map[row['terminal_type']] = []
                device_map[row['terminal_type']].append(row['device_id'])

            # 检查ODS数据是否存在
            cursor.execute(f"""
                SELECT COUNT(*) as cnt FROM ods_page_behavior 
                WHERE dt BETWEEN '{start_date}' AND '{end_date}'
            """)
            if cursor.fetchone()['cnt'] == 0:
                raise Exception(f"在 {start_date} 至 {end_date} 期间没有ODS数据")

            # 分页查询ODS数据并转换为DWD
            batch_size = 1000
            offset = 0
            total_processed = 0

            while True:
                cursor.execute(f"""
                    SELECT * FROM ods_page_behavior 
                    WHERE dt BETWEEN '{start_date}' AND '{end_date}'
                    LIMIT {batch_size} OFFSET {offset}
                """)
                ods_records = cursor.fetchall()
                if not ods_records:
                    break

                dwd_data = []
                for record in ods_records:
                    # 转换页面ID
                    page_id = page_map.get(record['page_name'])
                    source_page_id = page_map.get(record['source_page']) if record['source_page'] else None
                    target_page_id = page_map.get(record['target_page']) if record['target_page'] else None

                    # 转换时间ID
                    date_code = record['dt'].strftime("%Y%m%d")
                    time_id = time_map.get(date_code)

                    # 转换设备ID
                    device_id = random.choice(device_map[record['terminal_type']])

                    dwd_data.append({
                        "visitor_id": record['visitor_id'],
                        "time_id": time_id,
                        "page_id": page_id,
                        "page_type": record['page_type'],
                        "source_page_id": source_page_id,
                        "target_page_id": target_page_id,
                        "stay_duration": record['stay_duration'],
                        "device_id": device_id,
                        "is_order": record['is_order'],
                        "order_buyer_id": record['order_buyer_id'],
                        "dt": record['dt']
                    })

                # 插入当前批次
                self._batch_insert("dwd_user_page_visit_detail", dwd_data)
                total_processed += len(ods_records)
                offset += batch_size
                print(f"DWD层已处理 {total_processed} 条数据")

            print(f"DWD层数据生成完成，共处理 {total_processed} 条记录")

    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            print("数据库连接已关闭")


def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='生成ODS-DIM-DWD三层数据')
    parser.add_argument('--start-date', default='2025-01-01', help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', default='2025-01-31', help='结束日期 (YYYY-MM-DD)')
    parser.add_argument('--record-count', type=int, default=10000, help='ODS层数据量')
    args = parser.parse_args()

    # 创建生成器实例
    generator = MySQLDataGenerator()

    try:
        # 连接数据库
        if not generator.connect():
            return

        # 创建表结构
        generator.create_tables()

        # 生成DIM层数据
        print("开始生成DIM层数据...")
        generator.generate_dim_data()

        # 生成ODS层数据
        print("开始生成ODS层数据...")
        generator.generate_ods_data(args.start_date, args.end_date, args.record_count)

        # 生成DWD层数据
        print("开始生成DWD层数据...")
        generator.generate_dwd_data(args.start_date, args.end_date)

        print("所有数据生成流程完成！")

    except Exception as e:
        print(f"执行过程中出错: {str(e)}")
    finally:
        generator.close()


if __name__ == "__main__":
    main()
