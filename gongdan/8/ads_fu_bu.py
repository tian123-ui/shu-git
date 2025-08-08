import pandas as pd
import os
from datetime import datetime, timedelta

# 工单编号：大数据 - 电商数仓 - 08 - 商品主题营销工具客服专属优惠看板
# 功能：生成客服专属优惠ADS层数据，包含不分层及RFM分层两种指标计算方式

# 配置环境
os.environ["PYSPARK_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "D:\\ANACONDA\\envs\\day1\\python.exe"

# 创建存储目录
output_root = "mock_data"
if not os.path.exists(output_root):
    os.makedirs(output_root)
dws_root = "mock_data"  # DWS层路径
dim_root = "mock_data"  # DIM层路径
dwd_root = "mock_data"


# 文件存在性检查函数
def check_file_exists(file_path):
    if not os.path.exists(file_path):
        print(f"错误：文件不存在 - {file_path}")
        return False
    return True


# ------------------------------
# 辅助函数：计算RFM指标并分层
# ------------------------------
def calculate_rfm():
    """计算用户RFM指标并进行分层（高价值/潜力/一般/流失）"""
    # 读取核销数据（含用户消费信息）
    dwd_use_path = f"{dwd_root}/dwd_customer_service_promo_use.csv"
    if not check_file_exists(dwd_use_path):
        return None

    dwd_use = pd.read_csv(dwd_use_path)
    dwd_use['pay_time'] = pd.to_datetime(dwd_use['pay_time'])

    # 读取发送数据（关联用户ID）
    dwd_send_path = f"{dwd_root}/dwd_customer_service_promo_send.csv"
    dwd_send = pd.read_csv(dwd_send_path) if check_file_exists(dwd_send_path) else pd.DataFrame()
    if dwd_send.empty:
        return None

    # 检查发送表是否包含consumer_id，无则返回None
    if 'consumer_id' not in dwd_send.columns:
        print("错误：dwd_customer_service_promo_send.csv 中缺少 consumer_id 列，无法计算RFM")
        return None

    # 合并用户消费与发送数据
    user_data = pd.merge(
        dwd_send[['send_id', 'consumer_id']],
        dwd_use[['send_id', 'pay_time', 'pay_amount']],
        on='send_id',
        how='left'
    ).drop_duplicates()

    if user_data.empty:
        return None

    # 计算RFM指标（以当前时间为基准）
    today = datetime.now()
    rfm = user_data.groupby('consumer_id').agg(
        R=('pay_time', lambda x: (today - x.max()).days if not x.isna().all() else 999),  # 最近消费距今天数
        F=('send_id', 'count'),  # 消费频率（发送记录数）
        M=('pay_amount', lambda x: x.sum() if not x.isna().all() else 0)  # 消费金额
    ).reset_index()

    # RFM打分（1-5分，R值越小得分越高，F/M越大得分越高）
    rfm['R_score'] = pd.qcut(rfm['R'], 5, labels=[5, 4, 3, 2, 1], duplicates='drop')
    rfm['F_score'] = pd.qcut(rfm['F'], 5, labels=[1, 2, 3, 4, 5], duplicates='drop')
    rfm['M_score'] = pd.qcut(rfm['M'], 5, labels=[1, 2, 3, 4, 5], duplicates='drop')

    # 综合分层
    rfm['rfm_total'] = rfm['R_score'].astype(int) + rfm['F_score'].astype(int) + rfm['M_score'].astype(int)
    rfm['user_level'] = pd.cut(
        rfm['rfm_total'],
        bins=[0, 5, 8, 12, 15],
        labels=['流失用户', '一般用户', '潜力用户', '高价值用户']
    )

    return rfm[['consumer_id', 'R', 'F', 'M', 'user_level']]


# ------------------------------
# 1. 效果总览表（新增RFM分层指标）
# 包含：不分层（整体）+ 分层（按用户等级）两种统计方式
# ------------------------------
dws_daily_path = f"{dws_root}/dws_customer_service_promo_daily.csv"
if check_file_exists(dws_daily_path):
    dws_daily = pd.read_csv(dws_daily_path)
    dws_daily['stat_date'] = pd.to_datetime(dws_daily['stat_date']) if 'stat_date' in dws_daily.columns else None

    # 1.1 不分层：整体指标（修正变量名拼写错误）
    periods_types = [("day", "日"), ("7days", "7天"), ("30days", "30天")]
    overview_data = []
    # 变量名由period_types改为periods_types
    for period_code, period_name in periods_types:
        period_data = dws_daily.sort_values('stat_date', ascending=False).head(
            1 if period_code == "day" else 7 if period_code == "7days" else 30
        ) if 'stat_date' in dws_daily.columns else dws_daily

        total_send = int(period_data['total_send_count'].sum())
        total_use = int(period_data['total_use_count'].sum())
        valid_use = int(period_data['valid_use_count'].sum())
        use_rate = round(total_use / total_send * 100, 2) if total_send != 0 else 0

        overview_data.append({
            'stat_period': period_name,
            'user_level': '整体',  # 不分层标识
            'total_send_count': total_send,
            'total_use_count': total_use,
            'valid_use_count': valid_use,
            'use_rate': use_rate,
            'etl_load_time': datetime.now()
        })

    # 1.2 分层：按RFM用户等级统计
    rfm_data = calculate_rfm()
    if rfm_data is not None and not rfm_data.empty:
        # 关联发送数据获取用户等级对应的发送/核销量
        dwd_send = pd.read_csv(f"{dwd_root}/dwd_customer_service_promo_send.csv") if check_file_exists(
            f"{dwd_root}/dwd_customer_service_promo_send.csv") else pd.DataFrame()
        dwd_use = pd.read_csv(f"{dwd_root}/dwd_customer_service_promo_use.csv") if check_file_exists(
            f"{dwd_root}/dwd_customer_service_promo_use.csv") else pd.DataFrame()

        if not dwd_send.empty and 'consumer_id' in dwd_send.columns:
            # 发送量按用户等级统计
            send_with_level = pd.merge(dwd_send, rfm_data, on='consumer_id', how='left')
            send_level_agg = send_with_level.groupby('user_level').agg(
                total_send_count=('send_id', 'count')
            ).reset_index()

            # 核销量按用户等级统计
            use_with_level = pd.merge(
                dwd_use,
                dwd_send[['send_id', 'consumer_id']],
                on='send_id',
                how='left'
            ).merge(rfm_data, on='consumer_id', how='left')
            use_level_agg = use_with_level.groupby('user_level').agg(
                total_use_count=('use_id', 'count'),
                valid_use_count=('is_valid_use', lambda x: (x == 'true').sum())
            ).reset_index()

            # 合并发送与核销指标
            level_overview = pd.merge(send_level_agg, use_level_agg, on='user_level', how='left').fillna(0)
            level_overview['use_rate'] = round(
                level_overview['total_use_count'] / level_overview['total_send_count'] * 100, 2
            ).replace([float('inf'), -float('inf')], 0)

            # 补充周期信息（取最近30天）
            level_overview['stat_period'] = '30天'
            level_overview['etl_load_time'] = datetime.now()
            overview_data.extend(level_overview.to_dict('records'))

    # 保存效果总览表（含分层与不分层）
    ads_overview = pd.DataFrame(overview_data)
    ads_overview.to_csv(f"{output_root}/ads_customer_service_promo_overview.csv", index=False)
    print("效果总览表（含RFM分层）生成成功")
else:
    print("无法生成效果总览表，缺少DWS数据文件")

# ------------------------------
# 2. 活动效果表（补充工单注释）
# ------------------------------
# 工单编号：大数据 - 电商数仓 - 08 - 商品主题营销工具客服专属优惠看板
dws_activity_path = f"{dws_root}/dws_customer_service_promo_activity.csv"
if check_file_exists(dws_activity_path):
    dws_activity = pd.read_csv(dws_activity_path)

    dws_activity['use_rate'] = dws_activity.apply(
        lambda row: 0 if row['total_send_count'] == 0 else round(row['total_use_count'] / row['total_send_count'] * 100,
                                                                 2),
        axis=1
    )
    dws_activity = dws_activity.sort_values('use_rate', ascending=False)
    dws_activity['rank'] = dws_activity['use_rate'].rank(ascending=False, method='dense').astype(int)

    ads_activity_effect = dws_activity.rename(columns={
        'total_send_count': 'send_count',
        'total_use_count': 'use_count'
    })[['activity_id', 'activity_name', 'activity_status', 'send_count', 'use_count', 'use_rate', 'rank']]
    ads_activity_effect['etl_load_time'] = datetime.now()
    ads_activity_effect.to_csv(f"{output_root}/ads_customer_service_promo_activity_effect.csv", index=False)
    print("活动效果表生成成功")
else:
    print("无法生成活动效果表，缺少DWS数据文件")

# ------------------------------
# 3. 客服数据报表（补充RFM分层关联）
# 工单编号：大数据 - 电商数仓 - 08 - 商品主题营销工具客服专属优惠看板
# ------------------------------
dws_cs_path = f"{dws_root}/dws_customer_service_promo_cs.csv"
if check_file_exists(dws_cs_path):
    dws_cs = pd.read_csv(dws_cs_path)
    dws_cs_30d = dws_cs[dws_cs['stat_period'] == '30days'].copy()

    ads_cs_perf = dws_cs_30d[
        ['customer_service_id', 'cs_name', 'shop_id', 'send_count', 'use_count', 'use_rate']
    ].copy()
    ads_cs_perf['avg_promo_amount'] = 0.0
    ads_cs_perf['etl_load_time'] = datetime.now()
    ads_cs_perf.to_csv(f"{output_root}/ads_customer_service_performance.csv", index=False)
    print("客服数据报表生成成功")
else:
    print("无法生成客服数据报表，缺少DWS数据文件")

# ------------------------------
# 4. 发送明细汇总表（补充RFM用户等级）
# 工单编号：大数据 - 电商数仓 - 08 - 商品主题营销工具客服专属优惠看板
# ------------------------------
dwd_send_path = f"{dwd_root}/dwd_customer_service_promo_send.csv"
dwd_use_path = f"{dwd_root}/dwd_customer_service_promo_use.csv"
dim_cs_path = f"{dim_root}/dim_customer_service.csv"
if check_file_exists(dwd_send_path) and check_file_exists(dwd_use_path) and check_file_exists(dim_cs_path):
    dwd_send = pd.read_csv(dwd_send_path)
    dwd_use = pd.read_csv(dwd_use_path).rename(columns={'pay_time': 'use_time'})
    dim_cs = pd.read_csv(dim_cs_path)[['customer_service_id', 'cs_name']]

    # 关联客服信息
    dwd_send_with_cs = pd.merge(dwd_send, dim_cs, on='customer_service_id', how='left')

    # 检查是否存在consumer_id，无则添加默认值
    has_consumer_id = 'consumer_id' in dwd_send_with_cs.columns
    if not has_consumer_id:
        dwd_send_with_cs['consumer_id'] = 'unknown'  # 缺失时用unknown填充
        print("警告：dwd_customer_service_promo_send.csv 中缺少 consumer_id 列，已用unknown填充")

    # 关联RFM用户等级（仅当存在consumer_id时）
    rfm_data = calculate_rfm() if has_consumer_id else None
    if rfm_data is not None and not rfm_data.empty:
        dwd_send_with_cs = pd.merge(dwd_send_with_cs, rfm_data[['consumer_id', 'user_level']], on='consumer_id',
                                    how='left')
    else:
        dwd_send_with_cs['user_level'] = '未知'  # 无RFM数据时默认

    # 关联核销状态
    ads_send_detail = pd.merge(dwd_send_with_cs, dwd_use[['send_id', 'use_time']], on='send_id', how='left')
    ads_send_detail['use_status'] = ads_send_detail['use_time'].apply(
        lambda x: "已核销" if pd.notnull(x) else "未核销"
    )
    ads_send_detail['etl_load_time'] = datetime.now()

    # 选择需要的列（根据是否存在consumer_id调整）
    if has_consumer_id:
        selected_columns = [
            'send_id', 'activity_id', 'product_id', 'product_name', 'consumer_id', 'user_level',
            'customer_service_id', 'cs_name', 'send_time', 'use_status', 'use_time', 'etl_load_time'
        ]
    else:
        # 缺失consumer_id时移除该列
        selected_columns = [
            'send_id', 'activity_id', 'product_id', 'product_name', 'user_level',
            'customer_service_id', 'cs_name', 'send_time', 'use_status', 'use_time', 'etl_load_time'
        ]

    ads_send_detail = ads_send_detail[selected_columns]
    ads_send_detail.to_csv(f"{output_root}/ads_customer_service_promo_send_detail.csv", index=False)
    print("发送明细汇总表（含RFM用户等级）生成成功")
else:
    print("无法生成发送明细汇总表，缺少DWD/DIM数据文件")
