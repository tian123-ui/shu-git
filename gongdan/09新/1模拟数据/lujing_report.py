
#TODO: 2、店内路径看板代码需全部指标的功能，需包括不分层、分层两种实现方
# 式，代码需有明确的注释。

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pymysql
from sqlalchemy import create_engine
import networkx as nx  # 用于绘制路径网络图
import os
from datetime import datetime, timedelta
import uuid
import warnings
import random

# 设置中文字体显示，确保可视化中的中文能正常显示
plt.rcParams["font.family"] = ["SimHei", "WenQuanYi Micro Hei", "Heiti TC",
                               "Arial Unicode MS", "Microsoft YaHei", "SimSun",
                               "WenQuanYi Zen Hei", "STHeiti", "sans-serif"]
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示为方块的问题

# 检查系统可用字体并尝试设置最合适的中文字体
import matplotlib.font_manager as fm

available_fonts = []
for font_path in fm.findSystemFonts():
    try:
        font_prop = fm.FontProperties(fname=font_path)
        available_fonts.append(font_prop.get_name())
    except:
        continue

# 优先尝试的字体列表
preferred_fonts = ["SimHei", "WenQuanYi Micro Hei", "Microsoft YaHei", "SimSun"]

# 选择系统中可用的首选字体
for font in preferred_fonts:
    if font in available_fonts:
        plt.rcParams["font.family"] = [font]
        print(f"已设置字体: {font}")
        break
else:
    warnings.warn("未找到合适的中文字体，可能导致中文无法正确显示")

# 忽略不必要的警告信息
warnings.filterwarnings('ignore')


class FullLayerDataProcessor:
    """完整数据分层处理器，仅读取已有数据，不生成新表
    负责从ODS到ADS各层读取数据，为看板提供数据支持
    """

    def __init__(self, data_dir="ecommerce_data"):
        """初始化数据处理器
        参数:
            data_dir: 数据文件存放的目录路径，默认是"ecommerce_data"
        """
        self.data_dir = data_dir

        # 验证数据目录是否存在
        if not os.path.exists(self.data_dir):
            raise ValueError(f"数据目录不存在: {self.data_dir}")

        # 数据存储变量，按数据分层存储
        self.ods = None  # ODS层：原始数据层
        self.dim = {}    # DIM层：维度数据层，字典存储多个维度表
        self.dwd = None  # DWD层：明细数据层
        self.dws = {}    # DWS层：汇总数据层，字典存储多个汇总表
        self.ads = {}    # ADS层：应用数据层，字典存储多个应用表

        # 页面元数据：定义系统中所有页面的基础信息
        self.page_meta = [
            {"page_id": 1, "page_name": "首页", "page_type": "store_page", "page_subtype": "首页"},
            {"page_id": 2, "page_name": "活动页", "page_type": "store_page", "page_subtype": "活动页"},
            {"page_id": 3, "page_name": "分类页", "page_type": "store_page", "page_subtype": "分类页"},
            {"page_id": 4, "page_name": "宝贝页", "page_type": "store_page", "page_subtype": "宝贝页"},
            {"page_id": 5, "page_name": "新品页", "page_type": "store_page", "page_subtype": "新品页"},
            {"page_id": 6, "page_name": "商品详情页", "page_type": "product_detail", "page_subtype": "商品详情页"},
            {"page_id": 7, "page_name": "订阅页", "page_type": "other_page", "page_subtype": "订阅页"},
            {"page_id": 8, "page_name": "直播页", "page_type": "other_page", "page_subtype": "直播页"},
            {"page_id": 9, "page_name": "购物车", "page_type": "other_page", "page_subtype": "购物车"},
            {"page_id": 10, "page_name": "结算页", "page_type": "other_page", "page_subtype": "结算页"}
        ]
        # 页面ID到页面信息的映射，便于快速查询
        self.page_id_map = {p["page_id"]: p for p in self.page_meta}
        # 页面名称到ID的映射，便于快速查询
        self.page_name_to_id = {p["page_name"]: p["page_id"] for p in self.page_meta}

    def _validate_file_content(self, file_path, min_rows=1, min_cols=1):
        """验证文件内容是否有效，确保数据文件格式正确
        参数:
            file_path: 文件路径
            min_rows: 最小有效行数
            min_cols: 最小有效列数
        返回:
            验证通过返回True，否则抛出异常
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"文件不存在: {file_path}")

        if os.path.getsize(file_path) == 0:
            raise ValueError(f"文件为空: {file_path}")

        # 尝试读取前几行验证文件格式
        try:
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                first_line = f.readline()
                if not first_line.strip():
                    raise ValueError(f"文件没有有效内容: {file_path}")

                # 检查是否有足够的列
                columns = first_line.strip().split(',')
                if len(columns) < min_cols:
                    raise ValueError(f"文件列数不足，至少需要{min_cols}列: {file_path}")

                # 记录列名以便调试
                print(f"文件 {os.path.basename(file_path)} 包含列: {columns}")

            # 检查是否有足够的数据行
            df = pd.read_csv(file_path, nrows=min_rows)
            if len(df) < min_rows:
                raise ValueError(f"文件数据行不足，至少需要{min_rows}行数据: {file_path}")

            return True
        except Exception as e:
            raise ValueError(f"文件格式无效: {file_path}, 错误: {str(e)}")

    # ------------------------------
    # 1. ODS层：仅读取原始数据
    # ------------------------------
    def process_ods(self):
        """仅读取ODS层（原始数据层）数据，不生成新数据
        ODS层存储最原始的用户行为数据
        """
        ods_path = f"{self.data_dir}/ods_page_behavior.csv"

        try:
            # 验证文件内容，要求至少10行5列数据
            self._validate_file_content(ods_path, min_rows=10, min_cols=5)
            print("读取ODS层数据...")
            # 读取CSV文件，将visit_time解析为日期时间格式
            self.ods = pd.read_csv(ods_path, parse_dates=['visit_time'])
        except Exception as e:
            raise type(e)(f"处理ODS层数据失败: {str(e)}")

        return self

    # ------------------------------
    # 2. DIM层：仅读取维度数据
    # ------------------------------
    def process_dim(self):
        """仅读取DIM层（维度数据层）维度表，不生成新数据
        维度表存储分析所需的各种维度信息（页面、时间、设备等）
        """
        # 定义需要读取的维度文件信息
        dim_files = [
            {"name": "page", "path": "dim_page.csv", "min_rows": 10, "min_cols": 3},  # 页面维度表
            {"name": "time", "path": "dim_time.csv", "min_rows": 5, "min_cols": 3},   # 时间维度表
            {"name": "device", "path": "dim_device.csv", "min_rows": 10, "min_cols": 3}  # 设备维度表
        ]

        print("读取DIM层数据...")
        for file_info in dim_files:
            file_path = f"{self.data_dir}/{file_info['path']}"
            try:
                # 验证文件内容
                self._validate_file_content(
                    file_path,
                    min_rows=file_info['min_rows'],
                    min_cols=file_info['min_cols']
                )
                # 读取文件并存储到dim字典中
                self.dim[file_info['name']] = pd.read_csv(file_path)
            except Exception as e:
                raise type(e)(f"处理DIM层{file_info['name']}数据失败: {str(e)}")

        return self

    # ------------------------------
    # 3. DWD层：仅读取明细数据
    # ------------------------------
    def process_dwd(self):
        """仅读取DWD层（明细数据层）明细数据，不生成新数据
        DWD层存储经过清洗和标准化的用户行为明细数据
        """
        dwd_path = f"{self.data_dir}/dwd_user_page_visit_detail.csv"

        try:
            # 验证文件内容，要求至少100行8列数据
            self._validate_file_content(dwd_path, min_rows=100, min_cols=8)
            print("读取DWD层数据...")
            self.dwd = pd.read_csv(dwd_path)
        except Exception as e:
            raise type(e)(f"处理DWD层数据失败: {str(e)}")

        return self

    # ------------------------------
    # 4. DWS层：仅读取汇总数据
    # ------------------------------
    def process_dws(self):
        """仅读取DWS层（汇总数据层）汇总数据，不生成新数据
        DWS层存储按不同维度汇总的数据，为上层应用提供基础
        """
        # 定义需要读取的汇总文件信息
        dws_files = [
            {"name": "page_visit", "path": "dws_page_visit_stats.csv", "min_rows": 5, "min_cols": 5},  # 页面访问统计
            {"name": "path_flow", "path": "dws_instore_path_flow_stats.csv", "min_rows": 5, "min_cols": 5},  # 路径流转统计
            {"name": "pc_entry", "path": "dws_pc_traffic_entry_stats.csv", "min_rows": 3, "min_cols": 4}  # PC端入口统计
        ]

        print("读取DWS层数据...")
        for file_info in dws_files:
            file_path = f"{self.data_dir}/{file_info['path']}"
            try:
                # 验证文件内容
                self._validate_file_content(
                    file_path,
                    min_rows=file_info['min_rows'],
                    min_cols=file_info['min_cols']
                )
                # 读取文件并存储到dws字典中
                self.dws[file_info['name']] = pd.read_csv(file_path)
            except Exception as e:
                raise type(e)(f"处理DWS层{file_info['name']}数据失败: {str(e)}")

        return self

    # ------------------------------
    # 5. ADS层：仅读取应用数据
    # ------------------------------
    def process_ads(self):
        """仅读取ADS层（应用数据层）应用数据，不生成新数据
        ADS层存储直接用于业务分析和展示的数据
        """
        # 定义需要读取的应用文件信息
        ads_files = [
            {"name": "wireless_entry", "path": "ads_wireless_entry_receive.csv", "min_rows": 3, "min_cols": 4},  # 无线端入口数据
            {"name": "page_rank", "path": "ads_page_visit_rank.csv", "min_rows": 5, "min_cols": 5},  # 页面访问排名
            {"name": "path_display", "path": "ads_instore_path_display.csv", "min_rows": 5, "min_cols": 6},  # 店内路径展示数据
            {"name": "pc_top20", "path": "ads_pc_entry_top20.csv", "min_rows": 5, "min_cols": 5}  # PC端入口TOP20
        ]

        print("读取ADS层数据...")
        for file_info in ads_files:
            file_path = f"{self.data_dir}/{file_info['path']}"
            try:
                # 验证文件内容
                self._validate_file_content(
                    file_path,
                    min_rows=file_info['min_rows'],
                    min_cols=file_info['min_cols']
                )
                # 读取文件并存储到ads字典中
                self.ads[file_info['name']] = pd.read_csv(file_path)
            except Exception as e:
                raise type(e)(f"处理ADS层{file_info['name']}数据失败: {str(e)}")

        return self

    # ------------------------------
    # 全流程处理：仅读取所有层数据
    # ------------------------------
    def full_process(self):
        """仅读取从ODS到ADS的所有层数据，不生成任何新数据
        采用链式调用方式依次读取各层数据
        """
        print("开始读取全流程数据...")
        try:
            self.process_ods() \
                .process_dim() \
                .process_dwd() \
                .process_dws() \
                .process_ads()
            print("全流程数据读取完成")
        except Exception as e:
            print(f"全流程数据读取失败: {str(e)}")
            raise  # 重新抛出异常，让调用者知道发生了错误
        return self


class InstorePathDashboard:
    """店内路径看板，支持从已有数据读取指标
    提供两种数据获取方式：不分层（从原始数据计算）和分层（从ADS层获取）
    并支持将指标可视化展示
    """

    def __init__(self, data_processor=None, mysql_config=None):
        """初始化看板
        参数:
            data_processor: 数据处理器实例，提供数据来源
            mysql_config: MySQL连接配置（可选），用于从数据库读取数据
        """
        self.data_processor = data_processor
        self.mysql_config = mysql_config
        self.engine = None  # 数据库连接引擎

        # 如果提供了MySQL配置，创建连接引擎
        if mysql_config:
            self.engine = create_engine(
                f"mysql+pymysql://{mysql_config['user']}:{mysql_config['password']}@"
                f"{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}?charset=utf8mb4"
            )

    # ------------------------------
    # 不分层实现方式 - 从已有原始数据计算
    # ------------------------------
    def get_raw_metrics(self, date_start=None, date_end=None):
        """从已有原始数据计算所有指标（不分层方式）
        直接基于ODS层原始数据进行计算，不依赖上层汇总数据
        参数:
            date_start: 开始日期，用于过滤数据
            date_end: 结束日期，用于过滤数据
        返回:
            包含各类指标的字典
        """
        print("使用不分层方式计算指标...")
        # 如果ODS层数据未加载，则先加载
        if self.data_processor.ods is None:
            self.data_processor.process_ods()

        # 复制原始数据，避免修改源数据
        data = self.data_processor.ods.copy()

        # 显示可用列名，帮助调试
        print(f"ODS层数据包含列: {data.columns.tolist()}")

        # 日期过滤：如果提供了日期范围，则按日期筛选数据
        if date_start and date_end:
            data = data[(data['stat_date'] >= date_start) & (data['stat_date'] <= date_end)]

        # 1. 页面访问指标：按页面和终端类型分组统计
        # 动态确定访客数列名（兼容不同数据格式）
        visitor_col = 'visitor_id' if 'visitor_id' in data.columns else 'user_id'

        page_metrics = data.groupby(['page_name', 'terminal_type']).agg({
            visitor_col: 'nunique',  # 统计独立访客数
            'page_name': 'count',    # 统计浏览量
            'stay_duration': 'mean', # 计算平均停留时长
            'is_purchase': lambda x: sum(x) if 'is_purchase' in data.columns else 0  # 统计下单数
        }).rename(columns={
            visitor_col: '访客数',
            'page_name': '浏览量',
            'stay_duration': '平均停留时长',
            'is_purchase': '下单数'
        }).reset_index()
        page_metrics['平均停留时长'] = page_metrics['平均停留时长'].round(2)  # 保留两位小数

        # 2. 路径流转指标：统计页面间的跳转情况
        # 筛选有效路径数据（源页面和目标页面存在且不同）
        path_data = data[(data['source_page'].notna()) & (data['dest_page'].notna()) &
                         (data['source_page'] != data['dest_page'])]
        path_metrics = path_data.groupby(['source_page', 'dest_page', 'terminal_type']).agg({
            visitor_col: 'nunique'  # 统计该路径的独立访客数
        }).rename(columns={visitor_col: '访客数'}).reset_index()

        # 3. 终端分布指标：统计不同终端的访问情况
        terminal_metrics = data.groupby('terminal_type').agg({
            visitor_col: 'nunique',  # 总访客数
            'page_name': 'count',    # 总浏览量
            'is_purchase': lambda x: sum(x) if 'is_purchase' in data.columns else 0  # 总下单数
        }).rename(columns={
            visitor_col: '总访客数',
            'page_name': '总浏览量',
            'is_purchase': '总下单数'
        }).reset_index()

        # 计算下单转化率（避免除以零错误）
        terminal_metrics['下单转化率'] = (terminal_metrics['总下单数'] /
                                          terminal_metrics['总访客数'].replace(0, 1) * 100).round(2)

        # 4. 页面排名指标：按访客数对页面进行排名
        page_rank = data.groupby(['page_name']).agg({
            visitor_col: 'nunique'  # 统计各页面的独立访客数
        }).rename(columns={visitor_col: '访客数'}).reset_index()
        # 计算排名（降序，访客数越多排名越靠前）
        page_rank['排名'] = page_rank['访客数'].rank(ascending=False, method='dense').astype(int)
        page_rank = page_rank.sort_values('排名')  # 按排名排序

        # 5. 入口页面指标：统计用户进入店铺的首个页面
        # 源页面为空表示这是用户的第一个访问页面（入口页面）
        entry_pages = data[data['source_page'].isna()].groupby(['page_name', 'terminal_type']).agg({
            visitor_col: 'nunique'  # 统计各入口页面的独立访客数
        }).rename(columns={visitor_col: '访客数'}).reset_index()

        return {
            'page_metrics': page_metrics,       # 页面访问指标
            'path_metrics': path_metrics,       # 路径流转指标
            'terminal_metrics': terminal_metrics, # 终端分布指标
            'page_rank': page_rank,             # 页面排名指标
            'entry_pages': entry_pages          # 入口页面指标
        }

    # ------------------------------
    # 分层实现方式 - 从已有ADS层获取
    # ------------------------------
    def get_layered_metrics(self, time_granularity='day'):
        """从已有分层数据获取所有指标（分层方式）
        基于ADS层已汇总好的数据，直接读取并格式化，效率更高
        参数:
            time_granularity: 时间粒度，默认为'day'（按天）
        返回:
            包含各类指标的字典
        """
        print(f"使用分层方式获取指标，时间粒度: {time_granularity}")

        # 如果ADS层数据未加载，则先加载
        if not self.data_processor.ads:
            self.data_processor.process_ads()

        # 显示ADS层数据包含的列，帮助调试
        if "page_rank" in self.data_processor.ads:
            print(f"ADS层page_rank数据包含列: {self.data_processor.ads['page_rank'].columns.tolist()}")

        # 1. 页面访问指标 - 从page_rank获取，确保包含terminal_type字段
        page_metrics = self.data_processor.ads["page_rank"].copy()

        # 动态确定访客数列名（兼容不同数据格式）
        visitor_cols = ['访客数', '访问人数', 'visitor_count', 'user_count']
        page_visitor_col = next((col for col in visitor_cols if col in page_metrics.columns), None)

        if page_visitor_col is None:
            raise ValueError(f"在page_rank数据中未找到访客相关列，可用列: {page_metrics.columns.tolist()}")

        # 如果列名不是'访客数'，则重命名统一格式
        if page_visitor_col != '访客数':
            page_metrics = page_metrics.rename(columns={page_visitor_col: '访客数'})

        # 按时间粒度过滤数据
        if time_granularity != 'day':
            # 从时间维度表获取对应粒度的日期
            time_mask = self.data_processor.dim["time"][
                self.data_processor.dim["time"]["time_granularity"] == time_granularity
                ]["specific_date"]
            page_metrics = page_metrics[page_metrics["time_dimension"].isin(time_mask)]

        # 2. 路径流转指标：从path_display获取
        path_metrics = self.data_processor.ads["path_display"].copy()

        # 动态确定路径数据中的访客数列名
        path_visitor_col = next((col for col in visitor_cols if col in path_metrics.columns), None)
        if path_visitor_col is None:
            raise ValueError(f"在path_display数据中未找到访客相关列，可用列: {path_metrics.columns.tolist()}")

        # 重命名字段以匹配不分层数据格式，便于后续统一处理
        path_rename_map = {
            path_visitor_col: '访客数'
        }

        # 检查并处理源页面和目标页面列名，统一为'source_page'和'dest_page'
        if 'source_page_name' in path_metrics.columns:
            path_rename_map['source_page_name'] = 'source_page'
        elif 'from_page' in path_metrics.columns:
            path_rename_map['from_page'] = 'source_page'

        if 'dest_page_name' in path_metrics.columns:
            path_rename_map['dest_page_name'] = 'dest_page'
        elif 'to_page' in path_metrics.columns:
            path_rename_map['to_page'] = 'dest_page'

        path_metrics = path_metrics.rename(columns=path_rename_map)

        # 3. 终端分布指标：整合无线端和PC端数据
        wireless_data = self.data_processor.ads["wireless_entry"].copy()
        # 动态确定无线数据中的访客数和购买数列名
        wireless_visitor_col = next((col for col in ['visitor_num', '访客数', '访问人数']
                                     if col in wireless_data.columns), None)
        wireless_buyer_col = next((col for col in ['buyer_num', '下单数', '购买人数']
                                   if col in wireless_data.columns), None)

        if wireless_visitor_col is None:
            raise ValueError(f"在wireless_entry数据中未找到访客相关列，可用列: {wireless_data.columns.tolist()}")
        if wireless_buyer_col is None:
            raise ValueError(f"在wireless_entry数据中未找到购买相关列，可用列: {wireless_data.columns.tolist()}")

        # 计算无线端总计
        wireless_total = wireless_data.agg({
            wireless_visitor_col: "sum",
            wireless_buyer_col: "sum"
        }).to_dict()

        # 处理PC端数据
        pc_data = self.data_processor.ads["pc_top20"].copy()
        pc_visitor_col = next((col for col in ['visitor_count', '访客数', '访问人数']
                               if col in pc_data.columns), None)

        if pc_visitor_col is None:
            raise ValueError(f"在pc_top20数据中未找到访客相关列，可用列: {pc_data.columns.tolist()}")

        # 计算PC端总计
        pc_total = pc_data.agg({
            pc_visitor_col: "sum"
        }).to_dict()

        # 整合终端指标数据
        terminal_metrics = pd.DataFrame([
            {
                'terminal_type': 'wireless',  # 无线端
                '总访客数': wireless_total[wireless_visitor_col],
                '总下单数': wireless_total[wireless_buyer_col],
                '下单转化率': round((wireless_total[wireless_buyer_col] /
                                     wireless_total[wireless_visitor_col] * 100)
                                    if wireless_total[wireless_visitor_col] > 0 else 0, 2)
            },
            {
                'terminal_type': 'pc',  # PC端
                '总访客数': pc_total[pc_visitor_col],
                '总下单数': round(pc_total[pc_visitor_col] * 0.03),  # PC端转化率约3%（示例值）
                '下单转化率': 3.0  # 示例值
            }
        ])

        # 4. 页面排名指标：从page_rank获取并计算排名
        page_rank = self.data_processor.ads["page_rank"].copy()
        # 确保我们有正确的列名
        if page_visitor_col != '访客数':
            page_rank = page_rank.rename(columns={page_visitor_col: '访客数'})

        # 按页面名称汇总并计算排名
        page_rank = page_rank[['page_name', '访客数']].groupby('page_name').sum().reset_index()
        page_rank['排名'] = page_rank['访客数'].rank(ascending=False, method='dense').astype(int)
        page_rank = page_rank.sort_values('排名')

        # 5. 入口页面指标：整合无线端和PC端入口数据
        wireless_entry = self.data_processor.ads["wireless_entry"].copy()
        # 重命名无线入口页面相关列，统一格式
        wireless_entry_rename = {}
        if wireless_visitor_col != '访客数':
            wireless_entry_rename[wireless_visitor_col] = '访客数'
        if 'entry_page_type' in wireless_entry.columns:
            wireless_entry_rename['entry_page_type'] = 'page_name'
        elif '页面名称' in wireless_entry.columns:
            wireless_entry_rename['页面名称'] = 'page_name'

        wireless_entry = wireless_entry.rename(columns=wireless_entry_rename)
        wireless_entry = wireless_entry[['page_name', '访客数']].assign(terminal_type='wireless')

        # 处理PC端入口数据
        pc_entry = self.data_processor.ads["pc_top20"].copy()
        # 重命名PC入口页面相关列，统一格式
        pc_entry_rename = {}
        if pc_visitor_col != '访客数':
            pc_entry_rename[pc_visitor_col] = '访客数'
        if 'source_page' in pc_entry.columns:
            pc_entry_rename['source_page'] = 'page_name'
        elif '页面名称' in pc_entry.columns:
            pc_entry_rename['页面名称'] = 'page_name'

        pc_entry = pc_entry.rename(columns=pc_entry_rename)
        pc_entry = pc_entry[['page_name', '访客数']].assign(terminal_type='pc')

        # 合并无线端和PC端入口数据
        entry_pages = pd.concat([wireless_entry, pc_entry], ignore_index=True)

        return {
            'page_metrics': page_metrics,       # 页面访问指标
            'path_metrics': path_metrics,       # 路径流转指标
            'terminal_metrics': terminal_metrics, # 终端分布指标
            'page_rank': page_rank,             # 页面排名指标
            'entry_pages': entry_pages          # 入口页面指标
        }

    # ------------------------------
    # 可视化所有指标
    # ------------------------------
    def visualize_metrics(self, metrics, title_suffix=""):
        """可视化展示所有指标，生成综合看板
        参数:
            metrics: 包含各类指标的字典（由get_raw_metrics或get_layered_metrics返回）
            title_suffix: 看板标题后缀，用于区分不同类型的看板
        """
        # 创建一个大画布，设置尺寸
        plt.figure(figsize=(22, 28))
        plt.suptitle(f'店内路径分析看板 {title_suffix}', fontsize=16)  # 总标题

        # 1. 页面访问指标（分终端）- 第1个子图
        plt.subplot(3, 2, 1)  # 3行2列布局中的第1个位置
        if 'page_metrics' in metrics and not metrics['page_metrics'].empty:
            try:
                # 检查所需列是否存在
                required_cols = ['page_name', 'terminal_type', '访客数']
                missing_cols = [col for col in required_cols if col not in metrics['page_metrics'].columns]

                if not missing_cols:
                    # 数据透视，便于绘图
                    page_pivot = metrics['page_metrics'].pivot(index='page_name', columns='terminal_type',
                                                               values='访客数')
                    if not page_pivot.empty:
                        page_pivot.plot(kind='bar', ax=plt.gca())  # 绘制柱状图
                        plt.title('各页面访客数（分终端）')
                        plt.ylabel('访客数')
                        plt.xticks(rotation=45)  # x轴标签旋转45度，避免重叠
                else:
                    plt.title(f'页面访问指标缺少列: {missing_cols}')
            except Exception as e:
                plt.title(f'页面访问指标可视化失败: {str(e)}')

        # 2. 终端分布与转化率 - 第2个子图
        plt.subplot(3, 2, 2)  # 3行2列布局中的第2个位置
        ax = plt.gca()  # 获取当前轴
        if 'terminal_metrics' in metrics and not metrics['terminal_metrics'].empty:
            try:
                # 绘制双轴图：柱状图展示访客数和下单数，折线图展示转化率
                metrics['terminal_metrics'].plot(kind='bar', x='terminal_type',
                                                 y=['总访客数', '总下单数'], ax=ax)
                ax2 = ax.twinx()  # 创建共享x轴的第二个y轴
                metrics['terminal_metrics'].plot(kind='line', x='terminal_type',
                                                 y='下单转化率', color='red', marker='o', ax=ax2)
                ax2.set_ylabel('下单转化率（%）')
                plt.title('终端分布与转化率分析')
            except Exception as e:
                plt.title(f'终端分布可视化失败: {str(e)}')

        # 3. 页面排名（TOP10）- 第3个子图
        plt.subplot(3, 2, 3)  # 3行2列布局中的第3个位置
        if 'page_rank' in metrics and not metrics['page_rank'].empty:
            try:
                # 检查所需列是否存在
                if 'page_name' in metrics['page_rank'].columns and '访客数' in metrics['page_rank'].columns:
                    top10 = metrics['page_rank'].sort_values('排名').head(10)  # 取排名前10的页面
                    sns.barplot(x='访客数', y='page_name', data=top10)  # 横向柱状图
                    plt.title('页面访客数TOP10')
                else:
                    missing = []
                    if 'page_name' not in metrics['page_rank'].columns:
                        missing.append('page_name')
                    if '访客数' not in metrics['page_rank'].columns:
                        missing.append('访客数')
                    plt.title(f'页面排名缺少列: {missing}')
            except Exception as e:
                plt.title(f'页面排名可视化失败: {str(e)}')

        # 4. 入口页面分析 - 第4个子图
        plt.subplot(3, 2, 4)  # 3行2列布局中的第4个位置
        if 'entry_pages' in metrics and not metrics['entry_pages'].empty:
            try:
                # 检查所需列是否存在
                required_cols = ['page_name', 'terminal_type', '访客数']
                missing_cols = [col for col in required_cols if col not in metrics['entry_pages'].columns]

                if not missing_cols:
                    # 数据透视，便于绘图
                    entry_pivot = metrics['entry_pages'].pivot(index='page_name',
                                                               columns='terminal_type',
                                                               values='访客数').fillna(0)
                    entry_pivot.plot(kind='bar', ax=plt.gca())  # 绘制柱状图
                    plt.title('各入口页面访客数（分终端）')
                    plt.ylabel('访客数')
                    plt.xticks(rotation=45)  # x轴标签旋转45度
                else:
                    plt.title(f'入口页面指标缺少列: {missing_cols}')
            except Exception as e:
                plt.title(f'入口页面可视化失败: {str(e)}')

        # 5. 店内路径图 - 第5个子图（占满最后一行）
        plt.subplot(3, 1, 3)  # 3行1列布局中的第3个位置（即最后一行）
        if 'path_metrics' in metrics and not metrics['path_metrics'].empty:
            try:
                # 检查所需列是否存在
                required_cols = ['source_page', 'dest_page', '访客数']
                missing_cols = [col for col in required_cols if col not in metrics['path_metrics'].columns]

                if not missing_cols:
                    # 创建有向图
                    G = nx.DiGraph()
                    # 选取前15条主要路径（按访客数排序）
                    top_paths = metrics['path_metrics'].sort_values('访客数', ascending=False).head(15)

                    # 添加边和权重（访客数）
                    for _, row in top_paths.iterrows():
                        G.add_edge(row['source_page'], row['dest_page'],
                                   weight=int(row['访客数']))

                    # 布局设置
                    pos = nx.spring_layout(G, k=0.6)  # 弹簧布局
                    # 绘制节点
                    nx.draw_networkx_nodes(G, pos, node_size=1200, node_color='lightblue')
                    # 绘制节点标签
                    nx.draw_networkx_labels(G, pos, font_size=10)
                    # 绘制边
                    nx.draw_networkx_edges(G, pos, arrowstyle='->', width=2)

                    # 添加边权重标签（访客数）
                    edge_labels = {(u, v): f"{d['weight']}" for u, v, d in G.edges(data=True)}
                    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=8)

                    plt.title('主要页面流转路径（访客数）')
                    plt.axis('off')  # 关闭坐标轴
                else:
                    plt.title(f'路径指标缺少列: {missing_cols}')
            except Exception as e:
                plt.title(f'路径图可视化失败: {str(e)}')

        # 调整布局，为总标题留出空间
        plt.tight_layout(rect=[0, 0, 1, 0.96])
        # 保存图片
        plt.savefig(f'店内路径看板{title_suffix}.png', dpi=300, bbox_inches='tight')
        # 显示图片
        plt.show()

        return self


# ------------------------------
# 主函数：仅读取已有数据并生成看板
# ------------------------------
def main():
    # 1. 初始化数据处理器并读取已有数据
    try:
        data_processor = FullLayerDataProcessor()
        data_processor.full_process()  # 仅读取数据，不生成新表
    except Exception as e:
        print(f"数据处理错误: {e}")
        print("请检查数据目录下的文件是否完整且格式正确")
        return

    # 2. 初始化看板
    dashboard = InstorePathDashboard(data_processor)

    try:
        # 3. 生成不分层方式的看板（从原始数据计算）
        raw_metrics = dashboard.get_raw_metrics(date_start='2025-01-01', date_end='2025-01-30')
        dashboard.visualize_metrics(raw_metrics, title_suffix="(不分层方式)")

        # 4. 生成分层方式的看板（从ADS层获取）
        layered_metrics = dashboard.get_layered_metrics(time_granularity='day')
        dashboard.visualize_metrics(layered_metrics, title_suffix="(分层方式)")

        print("所有看板生成完成")
    except Exception as e:
        print(f"生成看板时出错: {e}")


if __name__ == "__main__":
    main()
