import pandas as pd  # 用于数据处理和分析的核心库
import numpy as np  # 用于数值计算
import random  # 用于生成随机数据，模拟用户行为
from datetime import datetime, timedelta  # 用于处理日期和时间
import uuid  # 用于生成唯一标识符（如访客ID）
import os  # 用于文件和目录操作


# 电商数仓流量主题数据生成脚本
# 严格遵循ODS→DIM→DWD→DWS→ADS分层架构
# 所有数值型字段使用round函数处理精度

class EcommerceFullLayerGenerator:
    """电商全分层数据生成器类，负责生成从ODS到ADS各层数据"""

    def __init__(self):
        """初始化配置，设置时间范围和页面元数据"""
        # 时间配置：限定为2025年1月1日至1月30日（共30天）
        self.start_date = datetime(2025, 1, 1)
        self.end_date = datetime(2025, 1, 30)
        # 生成每天的日期列表，用于随机选择访问日期
        self.dates = [self.start_date + timedelta(days=i) for i in range(30)]

        # 页面基础元数据（用于构建dim_page维度表）
        # 包含10种典型电商页面，分三类：店铺页、商品详情页、其他页
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
        # 提取所有页面ID，用于随机选择
        self.page_id_list = [p["page_id"] for p in self.page_meta]
        # 构建页面名称到ID的映射字典，用于后续ID转换
        self.page_name_to_id = {p["page_name"]: p["page_id"] for p in self.page_meta}

    # 1. 生成ODS层：用户行为日志表
    def build_ods_page_behavior(self, record_count=1000000):
        """
        生成ods_page_behavior表（原始用户行为数据）
        :param record_count: 生成的数据条数，默认100万条（符合验收标准）
        :return: 包含原始用户行为数据的DataFrame
        """
        print(f"正在生成ODS层数据 ({record_count}条)...")
        data = []  # 用于存储生成的每条记录

        # 循环生成指定数量的记录
        for _ in range(record_count):
            # 基础访问信息
            visit_date = random.choice(self.dates)  # 随机选择访问日期（1-30日）
            visitor_id = str(uuid.uuid4())  # 生成唯一访客ID（UUID格式）
            # 按7:3比例随机生成终端类型（无线端占比更高，符合实际业务）
            terminal = random.choices(["wireless", "pc"], weights=[0.7, 0.3])[0]

            # 页面信息：随机选择一个页面及其类型
            page = random.choice(self.page_meta)
            page_name = page["page_name"]
            page_type = page["page_type"]

            # 路径信息（模拟用户行为的多向性和不重复性）
            has_source = random.random() > 0.3  # 30%的访问无来源页面
            has_dest = random.random() > 0.2  # 20%的访问无去向页面
            # 若有来源/去向，随机选择一个页面；否则为None
            source_page = random.choice(self.page_meta)["page_name"] if has_source else None
            dest_page = random.choice(self.page_meta)["page_name"] if has_dest else None

            # 行为指标
            stay_duration = random.randint(5, 300)  # 停留时长：5-300秒（合理范围）
            is_purchase = random.random() < 0.03  # 3%的下单转化率（模拟真实电商场景）
            # 下单时buyer_id与visitor_id一致，否则为None
            buyer_id = visitor_id if is_purchase else None

            # 将生成的记录添加到列表
            data.append({
                "visitor_id": visitor_id,
                "visit_time": visit_date + timedelta(
                    hours=random.randint(9, 22),  # 访问时间集中在9:00-22:00（用户活跃时段）
                    minutes=random.randint(0, 59),
                    seconds=random.randint(0, 59)
                ),
                "page_name": page_name,
                "page_type": page_type,
                "source_page": source_page,
                "dest_page": dest_page,
                "stay_duration": stay_duration,
                "terminal_type": terminal,
                "is_purchase": is_purchase,
                "buyer_id": buyer_id,
                "stat_date": visit_date.strftime("%Y-%m-%d")  # 格式化日期，用于后续关联
            })

        # 将列表转换为DataFrame并返回
        return pd.DataFrame(data)

    # 2. 生成DIM层：维度表集合
    def build_dim_layers(self):
        """
        生成dim_page、dim_time、dim_device三个维度表
        :return: 包含三个维度表的字典
        """
        print("正在生成DIM层维度数据...")

        # 2.1 页面维度表：直接从page_meta转换而来，规范页面信息
        dim_page = pd.DataFrame(self.page_meta)

        # 2.2 时间维度表（支持多粒度分析）
        dim_time = []
        time_id = 1  # 时间ID自增起始值
        # 日粒度：每天一条记录
        for date in self.dates:
            dim_time.append({
                "time_id": time_id,
                "time_granularity": "day",  # 粒度：天
                "date_code": date.strftime("%Y%m%d"),  # 日期编码（如20250101）
                "specific_date": date.strftime("%Y-%m-%d")  # 具体日期（如2025-01-01）
            })
            time_id += 1
        # 7天粒度：每7天一个周期（覆盖整个1月）
        for i in range(0, 30, 7):
            end_idx = min(i + 6, 29)  # 最后一个周期可能不足7天
            dim_time.append({
                "time_id": time_id,
                "time_granularity": "7days",  # 粒度：7天
                "date_code": f"{self.dates[i].strftime('%Y%m%d')}-{self.dates[end_idx].strftime('%Y%m%d')}",
                "specific_date": f"{self.dates[i].strftime('%Y-%m-%d')}至{self.dates[end_idx].strftime('%Y-%m-%d')}"
            })
            time_id += 1
        # 月粒度：整个1月
        dim_time.append({
            "time_id": time_id,
            "time_granularity": "month",  # 粒度：月
            "date_code": self.start_date.strftime("%Y%m"),  # 月份编码（如202501）
            "specific_date": self.start_date.strftime("%Y年%m月")  # 具体月份（如2025年01月）
        })
        dim_time = pd.DataFrame(dim_time)

        # 2.3 设备维度表：模拟不同终端设备
        dim_device = []
        # 无线设备（1000个）：生成wireless_1到wireless_1000
        for i in range(1000):
            dim_device.append({
                "device_id": f"wireless_{i + 1}",
                "terminal_type": "wireless",
                "device_identifier": f"mobile_{i + 1}"  # 设备标识（如mobile_1）
            })
        # PC设备（500个）：生成pc_1到pc_500
        for i in range(500):
            dim_device.append({
                "device_id": f"pc_{i + 1}",
                "terminal_type": "pc",
                "device_identifier": f"computer_{i + 1}"  # 设备标识（如computer_1）
            })
        dim_device = pd.DataFrame(dim_device)

        # 返回三个维度表
        return {
            "dim_page": dim_page,
            "dim_time": dim_time,
            "dim_device": dim_device
        }

    # 3. 生成DWD层：用户页面访问明细
    def build_dwd_user_visit(self, ods_data, dim_time, dim_device):
        """
        生成dwd_user_page_visit_detail表（清洗关联后的明细数据）
        :param ods_data: ODS层原始数据
        :param dim_time: 时间维度表
        :param dim_device: 设备维度表
        :return: DWD层明细数据DataFrame
        """
        print("正在生成DWD层明细数据...")
        dwd = ods_data.copy()  # 复制ODS数据，避免修改原始数据

        # 转换页面名称为ID（标准化处理）
        dwd["page_id"] = dwd["page_name"].map(self.page_name_to_id)  # 当前页面ID
        dwd["source_page_id"] = dwd["source_page"].map(self.page_name_to_id)  # 来源页面ID
        dwd["dest_page_id"] = dwd["dest_page"].map(self.page_name_to_id)  # 去向页面ID

        # 关联时间ID（仅关联日粒度，因为明细数据是按天产生的）
        # 构建日期到时间ID的映射（日粒度）
        day_time_map = dict(zip(
            dim_time[dim_time["time_granularity"] == "day"]["specific_date"],
            dim_time[dim_time["time_granularity"] == "day"]["time_id"]
        ))
        dwd["visit_time_id"] = dwd["stat_date"].map(day_time_map)  # 关联时间ID

        # 关联设备ID（为每个访问分配一个设备）
        # 按终端类型分组设备ID
        device_groups = {
            "wireless": dim_device[dim_device["terminal_type"] == "wireless"]["device_id"].tolist(),
            "pc": dim_device[dim_device["terminal_type"] == "pc"]["device_id"].tolist()
        }
        # 为每条记录随机分配对应终端类型的设备ID
        dwd["device_id"] = dwd["terminal_type"].apply(
            lambda x: random.choice(device_groups[x])
        )

        # 保留需要的字段，去除冗余信息
        return dwd[[
            "visitor_id", "visit_time_id", "page_id", "page_type",
            "source_page_id", "dest_page_id", "stay_duration", "device_id",
            "is_purchase", "buyer_id", "stat_date"
        ]]

    # 4. 生成DWS层：汇总数据
    def build_dws_layers(self, dwd_data, dim_device):
        """
        生成DWS层三个汇总表
        :param dwd_data: DWD层明细数据
        :param dim_device: 设备维度表
        :return: 包含三个汇总表的字典
        """
        print("正在生成DWS层汇总数据...")

        # 关联设备类型（从设备ID映射到终端类型）
        device_type_map = dict(zip(dim_device["device_id"], dim_device["terminal_type"]))
        dwd_data["device_type"] = dwd_data["device_id"].map(device_type_map)

        # 4.1 页面访问指标汇总表：按时间、页面类型、设备类型聚合
        page_stats = dwd_data.groupby(["visit_time_id", "page_type", "device_type"]).agg({
            "visitor_id": "nunique",  # 访客数（去重）
            "page_id": "count",  # 浏览量（不去重，统计总访问次数）
            "stay_duration": "mean",  # 平均停留时长
            "buyer_id": lambda x: x.nunique()  # 下单买家数（去重）
        }).rename(columns={  # 重命名列名，更直观
            "visitor_id": "visitor_count",
            "page_id": "view_count",
            "stay_duration": "avg_stay_duration",
            "buyer_id": "purchase_buyer_count"
        }).reset_index()  # 重置索引，将分组字段转为列
        # 处理浮点精度：平均停留时长保留2位小数
        page_stats["avg_stay_duration"] = page_stats["avg_stay_duration"].apply(lambda x: round(x, 2))

        # 4.2 店内路径流转汇总表：统计页面间的流转访客数
        # 过滤条件：有来源、有去向、且来源≠去向（排除无效路径）
        path_stats = dwd_data[
            (dwd_data["source_page_id"].notna()) &
            (dwd_data["dest_page_id"].notna()) &
            (dwd_data["source_page_id"] != dwd_data["dest_page_id"])
            ].groupby(["visit_time_id", "device_type", "source_page_id", "dest_page_id"]).agg({
            "visitor_id": "nunique"  # 统计不重复访客数
        }).rename(columns={"visitor_id": "visitor_count"}).reset_index()

        # 4.3 PC端流量入口汇总表：统计PC端来源页面的访客数及占比
        pc_entry_stats = dwd_data[
            (dwd_data["device_type"] == "pc") &  # 仅PC端
            (dwd_data["source_page_id"].notna())  # 有来源页面
            ].groupby(["visit_time_id", "source_page_id"]).agg({
            "visitor_id": "nunique"  # 来源页面的访客数
        }).rename(columns={"visitor_id": "visitor_count"}).reset_index()

        # 计算来源占比（某来源访客数/总访客数）
        # 先计算每天PC端的总访客数
        total_pc = pc_entry_stats.groupby("visit_time_id")["visitor_count"].sum().reset_index()
        total_pc.columns = ["visit_time_id", "total_visitors"]  # 重命名列
        # 关联总访客数，计算占比
        pc_entry_stats = pc_entry_stats.merge(total_pc, on="visit_time_id")
        pc_entry_stats["source_ratio"] = round(
            pc_entry_stats["visitor_count"] / pc_entry_stats["total_visitors"] * 100, 2
        )

        # 返回三个汇总表
        return {
            "dws_page_visit_stats": page_stats,
            "dws_instore_path_flow_stats": path_stats,
            "dws_pc_traffic_entry_stats": pc_entry_stats
        }

    # 5. 生成ADS层：应用数据
    def build_ads_layers(self, dws_data, dim_page, dim_time):
        """
        生成ADS层四个应用表（直接支撑看板展示）
        :param dws_data: DWS层汇总数据
        :param dim_page: 页面维度表
        :param dim_time: 时间维度表
        :return: 包含四个应用表的字典
        """
        print("正在生成ADS层应用数据...")

        # 辅助映射：用于ID转名称/日期
        page_id_to_name = {p["page_id"]: p["page_name"] for p in self.page_meta}  # 页面ID→名称
        time_id_to_date = dict(zip(dim_time["time_id"], dim_time["specific_date"]))  # 时间ID→具体日期

        # 5.1 无线端入店与承接指标表
        # 筛选无线端数据
        wireless_entry = dws_data["dws_page_visit_stats"][
            dws_data["dws_page_visit_stats"]["device_type"] == "wireless"
            ].copy()
        # 关联时间维度（将time_id转为具体日期）
        wireless_entry["time_dimension"] = wireless_entry["visit_time_id"].map(time_id_to_date)
        # 按时间和页面类型聚合，得到访客数和下单买家数
        wireless_entry = wireless_entry.groupby(["time_dimension", "page_type"]).agg({
            "visitor_count": "sum",
            "purchase_buyer_count": "sum"
        }).reset_index().rename(columns={
            "page_type": "entry_page_type",  # 重命名为入店页面类型
            "visitor_count": "visitor_num",
            "purchase_buyer_count": "buyer_num"
        })

        # 5.2 页面访问排行表
        page_rank = dws_data["dws_page_visit_stats"].copy()
        # 关联时间维度
        page_rank["time_dimension"] = page_rank["visit_time_id"].map(time_id_to_date)
        # 关联页面名称（根据页面类型获取对应的页面名称）
        page_rank["page_name"] = page_rank["page_type"].apply(
            lambda x: next(p["page_name"] for p in self.page_meta if p["page_type"] == x)
        )
        # 计算排名：按时间、设备类型、页面类型分组，按访客数降序排名
        page_rank["rank"] = page_rank.groupby(["visit_time_id", "device_type", "page_type"])[
            "visitor_count"
        ].rank(ascending=False, method="dense").astype(int)

        # 5.3 店内路径展示表
        path_display = dws_data["dws_instore_path_flow_stats"].copy()
        # 关联时间维度
        path_display["time_dimension"] = path_display["visit_time_id"].map(time_id_to_date)
        # 转换来源/去向页面ID为名称
        path_display["source_page_name"] = path_display["source_page_id"].map(page_id_to_name)
        path_display["dest_page_name"] = path_display["dest_page_id"].map(page_id_to_name)
        # 计算来源占比：某去向访客数/来源页面总访客数
        # 先计算每个来源页面的总访客数
        source_totals = path_display.groupby(["time_dimension", "device_type", "source_page_name"])[
            "visitor_count"
        ].sum().reset_index().rename(columns={"visitor_count": "source_total"})
        # 关联总访客数，计算占比
        path_display = path_display.merge(source_totals, on=["time_dimension", "device_type", "source_page_name"])
        path_display["source_ratio"] = round(path_display["visitor_count"] / path_display["source_total"] * 100, 2)

        # 5.4 PC端流量入口TOP20表
        pc_top20 = dws_data["dws_pc_traffic_entry_stats"].copy()
        # 关联时间维度
        pc_top20["time_dimension"] = pc_top20["visit_time_id"].map(time_id_to_date)
        # 转换来源页面ID为名称
        pc_top20["source_page"] = pc_top20["source_page_id"].map(page_id_to_name)
        # 计算排名并取前20名
        pc_top20["rank"] = pc_top20.groupby("visit_time_id")["visitor_count"].rank(
            ascending=False, method="dense"
        ).astype(int)
        pc_top20 = pc_top20[pc_top20["rank"] <= 20]  # 仅保留前20名

        # 返回四个应用表
        return {
            "ads_wireless_entry_receive": wireless_entry,
            "ads_page_visit_rank": page_rank,
            "ads_instore_path_display": path_display,
            "ads_pc_entry_top20": pc_top20
        }

    # 统一保存CSV方法
    def save_table(self, df, table_name, output_dir="ecommerce_data"):
        """
        保存数据表到CSV文件，自动处理数值精度
        :param df: 要保存的DataFrame
        :param table_name: 表名（作为文件名）
        :param output_dir: 输出目录，默认为"ecommerce_data"
        """
        # 创建输出目录（如果不存在）
        os.makedirs(output_dir, exist_ok=True)
        file_path = f"{output_dir}/{table_name}.csv"

        # 处理数值精度：所有浮点型字段保留2位小数
        for col in df.select_dtypes(include=['float64']).columns:
            df[col] = df[col].apply(lambda x: round(x, 2))

        # 保存CSV文件
        df.to_csv(
            file_path,
            index=False,  # 不保存索引
            encoding='utf-8-sig',  # 支持中文显示
            float_format='%.2f'  # 浮点型格式化为2位小数
        )
        print(f"已保存: {file_path}")


# 执行生成流程
if __name__ == "__main__":
    # 创建数据生成器实例
    generator = EcommerceFullLayerGenerator()

    # 按顺序生成各层数据
    ods_data = generator.build_ods_page_behavior(1000000)  # 生成100万条原始数据
    dim_data = generator.build_dim_layers()  # 生成维度表
    dwd_data = generator.build_dwd_user_visit(  # 生成明细数据层
        ods_data,
        dim_data["dim_time"],
        dim_data["dim_device"]
    )
    dws_data = generator.build_dws_layers(  # 生成汇总数据层
        dwd_data,
        dim_data["dim_device"]
    )
    ads_data = generator.build_ads_layers(  # 生成应用数据层
        dws_data,
        dim_data["dim_page"],
        dim_data["dim_time"]
    )

    # 保存所有表到CSV文件
    generator.save_table(ods_data, "ods_page_behavior")

    for name, df in dim_data.items():
        generator.save_table(df, name)

    generator.save_table(dwd_data, "dwd_user_page_visit_detail")

    for name, df in dws_data.items():
        generator.save_table(df, name)

    for name, df in ads_data.items():
        generator.save_table(df, name)

    print("全部分层数据生成完成！")
