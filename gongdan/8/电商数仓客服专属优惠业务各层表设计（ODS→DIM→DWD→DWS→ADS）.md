### ODS 层（原始数据层）7

- `ods_customer_service.csv`：客服原始信息表
- `ods_customer_service_promo_activity.csv`：客服专属优惠活动原始表
- `ods_customer_service_promo_activity_item.csv`：客服优惠活动 - 商品关联原始表
- `ods_customer_service_promo_send.csv`：客服优惠发送原始表
- `ods_customer_service_promo_use.csv`：客服优惠核销原始表
- `ods_product.csv`：商品原始信息表
- `ods_sku.csv`：商品 SKU 原始信息表

### DIM 层（维度层）5

- `dim_activity_type.csv`：活动类型维度表
- `dim_customer_service.csv`：客服维度表
- `dim_product.csv`：商品维度表
- `dim_shop.csv`：店铺维度表
- `dim_sku.csv`：商品 SKU 维度表

### DWD 层（明细数据层）4

- `dwd_customer_service_promo_activity.csv`：客服优惠活动明细事实表
- `dwd_customer_service_promo_activity_item.csv`：客服优惠活动 - 商品关联明细事实表
- `dwd_customer_service_promo_send.csv`：客服优惠发送明细事实表
- `dwd_customer_service_promo_use.csv`：客服优惠核销明细事实表

### DWS 层（汇总数据层）4

- `dws_customer_service_promo_activity.csv`：客服优惠活动汇总表
- `dws_customer_service_promo_cs.csv`：客服维度优惠汇总表
- `dws_customer_service_promo_daily.csv`：客服优惠每日汇总表
- `dws_customer_service_promo_product.csv`：商品维度客服优惠汇总表

### ADS 层（应用数据层）3

- `ads_customer_service_performance.csv`：客服绩效应用表
- `ads_customer_service_promo_activity_effect.csv`：客服优惠活动效果应用表
- `ads_customer_service_promo_overview.csv`：客服优惠效果总览应用表


以下是基于数仓分层架构，结合电商客服专属优惠业务场景，对各层表结构与设计逻辑的梳理，覆盖 **ODS（原始数据层）、DIM（维度层）、DWD（明细数据层）、DWS（汇总数据层）、ADS（应用数据层）** 全链路：

---

### **一、ODS 层（原始数据层）**

**作用**：存储业务系统直接采集的原始数据，保留最细粒度、未经加工的业务记录，为后续分层处理提供基础。

#### 1. `ods_customer_service.csv`（客服信息表）

- **核心字段**：`customer_service_id`（客服 ID）、`cs_name`（客服姓名）、`shop_id`（所属店铺）、`create_time`（入职时间）
- **设计逻辑**：记录客服基础信息，与业务系统（如 CRM）直接同步，字段无清洗、保留原始格式。

#### 2. `ods_customer_service_promo_activity.csv`（客服优惠活动表）

- **核心字段**：`activity_id`（活动 ID）、`activity_name`（活动名称）、`activity_level`（活动级别：商品级 / SKU 级）、`promo_type`（优惠类型：固定 / 自定义）、`start_time`（开始时间）、`end_time`（结束时间）、`status`（状态：进行中 / 已结束）、`shop_id`（关联店铺）
- **设计逻辑**：采集活动创建时的原始配置，包含业务规则原始值（如 “自定义优惠需填金额上限”），字段未标准化。

#### 3. `ods_customer_service_promo_activity_item.csv`（活动 - 商品关联表）

- **核心字段**：`id`（关联 ID）、`activity_id`（活动 ID）、`product_id`（商品 ID）、`sku_id`（SKU ID，商品级活动为空）、`promo_amount`（优惠金额）、`limit_purchase_count`（限购次数）、`is_removed`（是否移出活动）
- **设计逻辑**：记录活动关联的商品 / SKU 关系，保留业务操作原始标记（如 “手动添加商品”“批量导入 SKU”）。

#### 4. `ods_customer_service_promo_send.csv`（优惠发送表）

- **核心字段**：`send_id`（发送 ID）、`activity_id`（活动 ID）、`product_id`（商品 ID）、`sku_id`（SKU ID）、`customer_service_id`（客服 ID）、`consumer_id`（消费者 ID）、`send_time`（发送时间）、`valid_duration`（有效时长：1-24 小时）、`actual_promo_amount`（实际优惠金额）
- **设计逻辑**：采集客服发送优惠的行为记录，包含原始业务限制（如 “24 小时内同一消费者不可重复发送”）。

#### 5. `ods_customer_service_promo_use.csv`（优惠核销表）

- **核心字段**：`use_id`（核销 ID）、`send_id`（发送 ID）、`order_id`（订单 ID）、`pay_time`（支付时间）、`pay_amount`（支付金额）、`purchase_count`（购买数量）
- **设计逻辑**：记录消费者使用优惠的核销行为，保留跨天核销场景的原始时间（如 “支付时间可能晚于发送时间”）。

#### 6. `ods_product.csv`（商品信息表）

- **核心字段**：`product_id`（商品 ID）、`product_name`（商品名称）、`price`（商品价格）、`status`（状态：在售 / 下架）、`shop_id`（所属店铺）
- **设计逻辑**：同步商品中心原始数据，字段未做业务逻辑加工。

#### 7. `ods_sku.csv`（SKU 信息表）

- **核心字段**：`sku_id`（SKU ID）、`product_id`（关联商品 ID）、`spec`（规格：颜色 / 尺码等）、`sku_price`（SKU 价格）
- **设计逻辑**：采集商品 SKU 原始属性，保留与商品的关联关系。

---

### **二、DIM 层（维度层）**

**作用**：对 ODS 层数据进行**标准化、维度化**处理，沉淀通用维度（如商品、店铺、客服），为上层分析提供一致的维度视角。

#### 1. `dim_customer_service.csv`（客服维度表）

- **核心字段**：`customer_service_id`（客服 ID）、`cs_name`（客服姓名）、`shop_id`（所属店铺）、`etl_update_time`（维度更新时间）
- **设计逻辑**：清洗 ODS 层客服信息，标准化字段格式（如姓名去重、店铺 ID 关联校验），沉淀稳定维度。

#### 2. `dim_product.csv`（商品维度表）

- **核心字段**：`product_id`（商品 ID）、`product_name`（商品名称）、`price`（商品价格）、`status`（状态：在售 / 下架）、`shop_id`（所属店铺）、`etl_update_time`（维度更新时间）
- **设计逻辑**：清洗 ODS 层商品信息，过滤无效状态（如 “下架商品标记”），统一价格精度（如保留两位小数）。

#### 3. `dim_shop.csv`（店铺维度表）

- **核心字段**：`shop_id`（店铺 ID）、`shop_name`（店铺名称）、`platform`（平台：淘宝 / 京东 / 拼多多）、`etl_update_time`（维度更新时间）
- **设计逻辑**：关联业务元数据，标准化平台名称（如 “淘宝” 统一替换为 “Taobao”），沉淀店铺基础属性。

#### 4. `dim_sku.csv`（SKU 维度表）

- **核心字段**：`sku_id`（SKU ID）、`product_id`（关联商品 ID）、`spec`（规格：颜色 / 尺码）、`sku_price`（SKU 价格）、`etl_update_time`（维度更新时间）
- **设计逻辑**：清洗 ODS 层 SKU 信息，关联商品维度表补充商品名称，标准化规格描述（如 “黑色” 统一格式）。

#### 5. `dim_activity_type.csv`（活动类型维度表）

- **核心字段**：`activity_level`（活动级别）、`promo_type`（优惠类型）、`desc`（业务描述）、`etl_update_time`（维度更新时间）
- **设计逻辑**：对活动级别（商品级 / SKU 级）、优惠类型（固定 / 自定义）进行标准化映射（如 “商品级” 映射为`product_level`），沉淀业务枚举值。

---

### **三、DWD 层（明细数据层）**

**作用**：对 ODS 层数据进行**清洗、关联、标准化**，保留最细粒度的业务事实，为上层汇总提供干净的明细数据。

#### 1. `dwd_customer_service_promo_activity.csv`（活动明细事实表）

- **核心字段**：`activity_id`（活动 ID）、`activity_name`（活动名称）、`activity_level`（标准化后：product_level/sku_level）、`promo_type`（标准化后：fixed/custom）、`start_time`（时间戳）、`end_time`（时间戳）、`status`（标准化后：ongoing/ended）、`shop_id`（关联店铺）、`shop_name`（关联店铺名称）、`etl_load_time`（加工时间）
- **设计逻辑**：
    - 关联 `dim_activity_type` 标准化活动级别、优惠类型；
    - 关联 `dim_shop` 补充店铺名称；
    - 清洗时间格式（如字符串转时间戳）、状态值（如 “进行中” 映射为`ongoing`）。

#### 2. `dwd_customer_service_promo_activity_item.csv`（活动 - 商品关联明细事实表）

- **核心字段**：`id`（关联 ID）、`activity_id`（活动 ID）、`product_id`（商品 ID）、`product_name`（商品名称）、`sku_id`（SKU ID）、`sku_spec`（SKU 规格）、`promo_amount`（优惠金额，截断后≤5000）、`limit_purchase_count`（限购次数）、`is_removed`（布尔值：是否移出）、`create_time`（时间戳）、`etl_load_time`（加工时间）
- **设计逻辑**：
    - 关联 `dim_product` 补充商品名称；
    - 关联 `dim_sku` 补充 SKU 规格；
    - 清洗金额（如 “优惠金额> 5000 截断为 5000”）、布尔值（如 “是 / 否” 转`True/False`）。

#### 3. `dwd_customer_service_promo_send.csv`（优惠发送明细事实表）

- **核心字段**：`send_id`（发送 ID）、`activity_id`（活动 ID）、`product_id`（商品 ID）、`product_name`（商品名称）、`sku_id`（SKU ID）、`customer_service_id`（客服 ID）、`cs_name`（客服姓名）、`consumer_id`（消费者 ID）、`actual_promo_amount`（优惠金额）、`valid_duration`（清洗后：1-24 小时）、`send_time`（时间戳）、`expire_time`（过期时间：send_time+valid_duration）、`is_expired`（布尔值：是否过期）、`etl_load_time`（加工时间）
- **设计逻辑**：
    - 关联 `dim_customer_service` 补充客服姓名；
    - 关联 `dim_product` 补充商品名称；
    - 清洗有效期（如 “<1 小时设为 1，>24 小时设为 24”）；
    - 计算过期时间、是否过期标记。

#### 4. `dwd_customer_service_promo_use.csv`（优惠核销明细事实表）

- **核心字段**：`use_id`（核销 ID）、`send_id`（发送 ID）、`order_id`（订单 ID）、`pay_time`（时间戳）、`pay_amount`（支付金额）、`purchase_count`（购买数量）、`activity_id`（关联活动 ID）、`product_id`（关联商品 ID）、`consumer_id`（消费者 ID）、`is_valid_use`（布尔值：支付时间≤过期时间）、`etl_load_time`（加工时间）
- **设计逻辑**：
    - 关联 `dwd_customer_service_promo_send` 补充活动、商品、消费者 ID；
    - 计算核销有效性（`pay_time` 是否在有效期内）；
    - 清洗支付时间格式（字符串转时间戳）。

---

### **四、DWS 层（汇总数据层）**

**作用**：对 DWD 层明细数据进行 **轻度汇总**，按通用维度（如日期、店铺、活动、客服）聚合，生成可直接用于分析的指标。

#### 1. `dws_customer_service_promo_daily.csv`（每日汇总表）

- **核心字段**：`stat_date`（统计日期）、`shop_id`（店铺 ID）、`shop_name`（店铺名称）、`platform`（平台）、`total_send_count`（当日发送次数）、`total_use_count`（当日核销次数）、`valid_use_count`（有效核销次数）、`use_rate`（核销率：valid_use_count/total_send_count）、`etl_update_time`（加工时间）
- **设计逻辑**：
    - 按 `stat_date`（发送日期）+`shop_id` 聚合；
    - 关联 `dim_shop` 补充店铺名称、平台；
    - 计算核销率（处理除数为 0 场景，如 `total_send_count=0` 时 `use_rate=0`）。

#### 2. `dws_customer_service_promo_activity.csv`（活动汇总表）

- **核心字段**：`activity_id`（活动 ID）、`activity_name`（活动名称）、`activity_level`（活动级别）、`promo_type`（优惠类型）、`activity_status`（活动状态）、`total_send_count`（总发送次数）、`total_use_count`（总核销次数）、`7d_send_count`（近 7 天发送次数）、`7d_use_count`（近 7 天核销次数）、`use_rate`（核销率）、`etl_update_time`（加工时间）
- **设计逻辑**：
    - 按 `activity_id` 聚合，关联 `dwd_customer_service_promo_activity` 补充活动属性；
    - 区分 “近 7 天”“总周期” 统计发送、核销次数；
    - 支持活动状态（进行中 / 已结束）筛选。

#### 3. `dws_customer_service_promo_cs.csv`（客服汇总表）

- **核心字段**：`customer_service_id`（客服 ID）、`cs_name`（客服姓名）、`shop_id`（店铺 ID）、`stat_period`（统计周期：day/7days/30days）、`send_count`（周期内发送次数）、`use_count`（周期内核销次数）、`use_rate`（核销率）、`etl_update_time`（加工时间）
- **设计逻辑**：
    - 按 `customer_service_id`+`stat_period` 聚合；
    - 支持多周期（日 / 7 天 / 30 天）统计；
    - 关联 `dim_customer_service` 补充客服姓名、店铺 ID。

#### 4. `dws_customer_service_promo_product.csv`（商品汇总表）

- **核心字段**：`product_id`（商品 ID）、`product_name`（商品名称）、`sku_id`（SKU ID）、`sku_spec`（SKU 规格）、`total_send_count`（总发送次数）、`total_use_count`（总核销次数）、`use_rate`（核销率）、`etl_update_time`（加工时间）
- **设计逻辑**：
    - 按 `product_id`+`sku_id` 聚合；
    - 关联 `dim_product` 补充商品名称，关联 `dim_sku` 补充 SKU 规格；
    - 支持商品级、SKU 级指标拆分。

---

### **五、ADS 层（应用数据层）**

**作用**：针对**业务场景**（如客服优惠看板、活动效果分析），对 DWS 层指标进行 **再加工**，生成可直接用于可视化的结果。

#### 1. `ads_customer_service_promo_overview.csv`（效果总览表）

- **核心字段**：`stat_period`（统计周期：日 / 7 天 / 30 天）、`total_send_count`（周期内总发送次数）、`total_use_count`（周期内核销次数）、`valid_use_count`（有效核销次数）、`use_rate`（核销率）、`etl_load_time`（加工时间）
- **设计逻辑**：
    - 按 `stat_period` 聚合，筛选 “近 1 天”“近 7 天”“近 30 天” 数据；
    - 直接对接看板 “效果总览” 模块，支持时间周期切换。

#### 2. `ads_customer_service_promo_activity_effect.csv`（活动效果表）

- **核心字段**：`activity_id`（活动 ID）、`activity_name`（活动名称）、`activity_level`（活动级别）、`promo_type`（优惠类型）、`activity_status`（活动状态）、`send_count`（总发送次数）、`use_count`（总核销次数）、`use_rate`（核销率）、`rank`（活动核销率排名）、`etl_load_time`（加工时间）
- **设计逻辑**：
    - 基于 `dws_customer_service_promo_activity`，补充活动排名（按 `use_rate` 降序）；
    - 支持按活动状态（进行中 / 已结束）筛选，对接看板 “活动效果” 模块。

#### 3. `ads_customer_service_performance.csv`（客服绩效表）

- **核心字段**：`customer_service_id`（客服 ID）、`cs_name`（客服姓名）、`shop_id`（店铺 ID）、`stat_period`（统计周期：30days）、`send_count`（30 天发送次数）、`use_count`（30 天核销次数）、`use_rate`（核销率）、`avg_promo_amount`（平均优惠金额）、`etl_load_time`（加工时间）
- **设计逻辑**：
    - 基于 `dws_customer_service_promo_cs`，筛选 “30 天” 周期数据；
    - 补充 “平均优惠金额”（需关联 `dwd_customer_service_promo_send` 计算）；
    - 对接看板 “客服数据” 模块。

#### 4. `dws_customer_service_promo_product.csv`（商品汇总表）

- **核心字段**：`product_id`（商品 ID）、`product_name`（商品名称）、`sku_id`（SKU ID）、`sku_spec`（SKU 规格）、`total_send_count`（总发送次数）、`total_use_count`（总核销次数）、`use_rate`（核销率）、`etl_update_time`（加工时间）
- **设计逻辑**：
    - 按 `product_id`+`sku_id` 聚合，关联 `dim_product` 补充

