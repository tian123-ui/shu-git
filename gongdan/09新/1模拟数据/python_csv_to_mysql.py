import pandas as pd  # 用于数据处理和CSV文件读取
import pymysql  # 用于MySQL数据库连接
from sqlalchemy import create_engine  # 用于创建高效的数据库连接引擎

# MySQL 数据库连接信息配置
# 这些参数定义了连接数据库所需的所有信息
mysql_config = {
    "host": "cdh03",  # 数据库服务器主机地址（此处为示例地址）
    "port": 3306,  # 数据库端口号（MySQL默认端口为3306）
    "user": "root",  # 数据库登录用户名
    "password": "root",  # 数据库登录密码（生产环境需使用安全密码）
    "database": "gon09",  # 目标数据库名称，已指定为gon09
    "charset": "utf8mb4"  # 字符集设置，utf8mb4支持所有Unicode字符（包括emoji）
}

# 创建数据库连接引擎
# 使用SQLAlchemy的create_engine构建连接字符串，格式为：
# mysql+pymysql://用户名:密码@主机:端口/数据库名?字符集
engine = create_engine(
    f"mysql+pymysql://{mysql_config['user']}:{mysql_config['password']}@"
    f"{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"
    f"?charset={mysql_config['charset']}"
)

# CSV 文件路径配置
csv_dir = "ecommerce_data"  # CSV文件存放的目录，默认为当前目录下的ecommerce_data子目录
# 需要导入的CSV文件列表，包含了数仓各分层的数据文件
csv_files = [
    "ads_instore_path_display.csv",  # ADS层：店内路径展示数据（直接用于路径看板）
    "ads_page_visit_rank.csv",  # ADS层：页面访问排名数据
    "ads_pc_entry_top20.csv",  # ADS层：PC端流量入口TOP20数据
    "ads_wireless_entry_receive.csv",  # ADS层：无线端入店与承接指标数据
    "dim_device.csv",  # DIM层：设备维度表（终端类型等信息）
    "dim_page.csv",  # DIM层：页面维度表（页面ID与名称映射）
    "dim_time.csv",  # DIM层：时间维度表（多粒度时间信息）
    "dwd_user_page_visit_detail.csv",  # DWD层：用户页面访问明细数据
    "dws_instore_path_flow_stats.csv",  # DWS层：店内路径流转汇总数据
    "dws_page_visit_stats.csv",  # DWS层：页面访问指标汇总数据
    "dws_pc_traffic_entry_stats.csv",  # DWS层：PC端流量入口汇总数据
    "ods_page_behavior.csv"  # ODS层：原始用户行为日志数据
]

# 遍历CSV文件列表，逐个导入数据库
for csv_file in csv_files:
    # 构建完整的CSV文件路径（目录+文件名）
    file_path = f"{csv_dir}/{csv_file}"
    # 生成表名：将文件名中的.csv后缀去除，保持表名与文件名一致
    table_name = csv_file.replace(".csv", "")

    try:
        # 读取CSV文件，使用utf-8编码确保中文正常读取
        df = pd.read_csv(file_path, encoding="utf-8")

        # 将数据写入MySQL数据库
        # to_sql方法是pandas提供的高效写入数据库的功能
        df.to_sql(
            name=table_name,  # 目标表名
            con=engine,  # 数据库连接引擎
            if_exists='replace',  # 表存在时的处理策略：replace表示替换现有表
            index=False  # 不将DataFrame的索引列导入数据库
        )

        # 打印成功信息
        print(f"成功将 {csv_file} 数据加载到 {table_name} 表")

    except Exception as e:
        # 捕获并打印导入过程中的错误信息
        print(f"加载 {csv_file} 数据失败，错误：{e}")
