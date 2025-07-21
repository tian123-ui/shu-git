from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


def get_spark_session():
    spark = SparkSession.builder \
        .appName("HiveETL") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE DATABASE IF NOT EXISTS gmall")
    spark.sql("USE gmall")
    return spark


def create_dim_sku_table(spark):
    """创建SKU维度表(如果不存在)"""
    # 先删除表（如果存在）
    spark.sql("DROP TABLE IF EXISTS gmall.dim_sku_full")

    # 然后创建新表（单独执行）
    spark.sql("""
    CREATE TABLE gmall.dim_sku_full (
        id string COMMENT 'SKU_ID',
        price decimal(16,2) COMMENT '价格',
        sku_name string COMMENT '商品名称',
        sku_desc string COMMENT '商品描述',
        weight decimal(16,2) COMMENT '重量',
        is_sale boolean COMMENT '是否在售',
        spu_id string COMMENT 'SPU编号',
        spu_name string COMMENT 'SPU名称',
        category3_id string COMMENT '三级分类ID',
        category3_name string COMMENT '三级分类名称',
        category2_id string COMMENT '二级分类id',
        category2_name string COMMENT '二级分类名称',
        category1_id string COMMENT '一级分类ID',
        category1_name string COMMENT '一级分类名称',
        tm_id string COMMENT '品牌ID',
        tm_name string COMMENT '品牌名称',
        attrs array<struct<
            attr_id:string,
            value_id:string,
            attr_name:string,
            value_name:string>> COMMENT '属性',
        sale_attrs array<struct<
            sale_attr_id:string,
            sale_attr_value_id:string,
            sale_attr_name:string,
            sale_attr_value_name:string>> COMMENT '销售属性',
        create_time string COMMENT '创建时间'
    )
    PARTITIONED BY (dt string)
    STORED AS ORC
    """)


def execute_hive_insert(partition_date: str, tableName):
    spark = get_spark_session()
    create_dim_sku_table(spark)

    # 显示完整表结构
    print(f"[INFO] 目标表 {tableName} 完整结构：")
    spark.sql(f"desc formatted {tableName}").show(n=50, truncate=False)

    # 构建SQL - 添加了is_sale字段的类型转换
    select_sql = f"""
with
    sku as
        (
            select
                id,
                price,
                sku_name,
                sku_desc,
                weight,
                -- 将int类型的is_sale转换为boolean
                case when is_sale = 1 then true else false end as is_sale,
                spu_id,
                category3_id,
                tm_id,
                create_time
            from ods_sku_info
            where dt='{partition_date}'
        ),
    spu as
        (
            select
                id,
                spu_name
            from ods_spu_info
            where dt='{partition_date}'
        ),
    c3 as
        (
            select
                id,
                name,
                category2_id
            from ods_base_category3
            where dt='{partition_date}'
        ),
    c2 as
        (
            select
                id,
                name,
                category1_id
            from ods_base_category2
            where dt='{partition_date}'
        ),
    c1 as
        (
            select
                id,
                name
            from ods_base_category1
            where dt='{partition_date}'
        ),
    tm as
        (
            select
                id,
                tm_name
            from ods_base_trademark
            where dt='{partition_date}'
        ),
    attr as
        (
            select
                sku_id,
                collect_set(named_struct('attr_id',attr_id,'value_id',value_id,'attr_name',attr_name,'value_name',value_name)) attrs
            from ods_sku_attr_value
            where dt='{partition_date}'
            group by sku_id
        ),
    sale_attr as
        (
            select
                sku_id,
                collect_set(named_struct('sale_attr_id',sale_attr_id,'sale_attr_value_id',sale_attr_value_id,'sale_attr_name',sale_attr_name,'sale_attr_value_name',sale_attr_value_name)) sale_attrs
            from ods_sku_sale_attr_value
            where dt='{partition_date}'
            group by sku_id
        )
insert overwrite table {tableName} partition(dt='{partition_date}')
select
    sku.id,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    sku.is_sale,
    sku.spu_id,
    spu.spu_name,
    sku.category3_id,
    c3.name as category3_name,
    c3.category2_id,
    c2.name as category2_name,
    c2.category1_id,
    c1.name as category1_name,
    sku.tm_id,
    tm.tm_name,
    attr.attrs,
    sale_attr.sale_attrs,
    sku.create_time
from sku
left join spu on sku.spu_id=spu.id
left join c3 on sku.category3_id=c3.id
left join c2 on c3.category2_id=c2.id
left join c1 on c2.category1_id=c1.id
left join tm on sku.tm_id=tm.id
left join attr on sku.id=attr.sku_id
left join sale_attr on sku.id=sale_attr.sku_id
    """

    print(f"[INFO] 执行SQL：{partition_date}")
    spark.sql(select_sql)

    # 验证数据
    print(f"[INFO] 验证数据：")
    spark.sql(f"SELECT * FROM {tableName} WHERE dt='{partition_date}'").show(5, truncate=False)


if __name__ == "__main__":
    execute_hive_insert('20250701', 'dim_sku_full')