/* =================================================
   DATABASES
================================================= */

CREATE DATABASE IF NOT EXISTS star;
CREATE DATABASE IF NOT EXISTS snowflake;
CREATE DATABASE IF NOT EXISTS reports;


/* =================================================
   ODS (до звезды)
================================================= */

CREATE TABLE IF NOT EXISTS star.customer_ods
(
    id UInt64,
    customer_first_name Nullable(String),
    customer_last_name Nullable(String),
    customer_age Nullable(Int32),
    customer_email Nullable(String),
    customer_country Nullable(String),
    customer_postal_code Nullable(String),
    customer_pet_type Nullable(String),
    customer_pet_name Nullable(String),
    customer_pet_breed Nullable(String)
)
ENGINE = MergeTree
ORDER BY id;


CREATE TABLE IF NOT EXISTS star.product_ods
(
    id UInt64,

    seller_first_name Nullable(String),
    seller_last_name Nullable(String),
    seller_email Nullable(String),
    seller_country Nullable(String),
    seller_postal_code Nullable(String),

    product_name Nullable(String),
    product_category Nullable(String),
    product_price Nullable(Decimal(10,2)),
    product_quantity Nullable(Int32),

    store_name Nullable(String),
    store_location Nullable(String),
    store_city Nullable(String),
    store_state Nullable(String),
    store_country Nullable(String),
    store_phone Nullable(String),
    store_email Nullable(String),
    pet_category Nullable(String),

    product_weight Nullable(Decimal(10,2)),
    product_color Nullable(String),
    product_size Nullable(String),
    product_brand Nullable(String),
    product_material Nullable(String),
    product_description Nullable(String),
    product_rating Nullable(Decimal(4,2)),
    product_reviews Nullable(Int32),
    product_release_date Nullable(String),
    product_expiry_date Nullable(String),

    supplier_name Nullable(String),
    supplier_contact Nullable(String),
    supplier_email Nullable(String),
    supplier_phone Nullable(String),
    supplier_address Nullable(String),
    supplier_city Nullable(String),
    supplier_country Nullable(String)
)
ENGINE = MergeTree
ORDER BY id;


CREATE TABLE IF NOT EXISTS star.sale_ods
(
    id UInt64,
    customer_id UInt64,
    product_id UInt64,
    sale_date Nullable(String),
    sale_quantity Nullable(Int32),
    sale_total_price Nullable(Decimal(12,2))
)
ENGINE = MergeTree
ORDER BY id;


/* =================================================
    / SNOWFLAKE
================================================= */

CREATE TABLE IF NOT EXISTS snowflake.pet_dds
(
    id UInt64,
    customer_pet_type Nullable(String),
    customer_pet_name Nullable(String),
    customer_pet_breed Nullable(String)
)
ENGINE = MergeTree
ORDER BY id;


CREATE TABLE IF NOT EXISTS snowflake.customer_dds
(
    id UInt64,
    customer_first_name Nullable(String),
    customer_last_name Nullable(String),
    customer_age Nullable(Int32),
    customer_email Nullable(String),
    customer_country Nullable(String),
    customer_postal_code Nullable(String),
    pet_id Nullable(UInt64)
)
ENGINE = MergeTree
ORDER BY id;


CREATE TABLE IF NOT EXISTS snowflake.seller_dds
(
    id UInt64,
    seller_first_name Nullable(String),
    seller_last_name Nullable(String),
    seller_email Nullable(String),
    seller_country Nullable(String),
    seller_postal_code Nullable(String)
)
ENGINE = MergeTree
ORDER BY id;


CREATE TABLE IF NOT EXISTS snowflake.store_dds
(
    id UInt64,
    store_name Nullable(String),
    store_location Nullable(String),
    store_city Nullable(String),
    store_state Nullable(String),
    store_country Nullable(String),
    store_phone Nullable(String),
    store_email Nullable(String)
)
ENGINE = MergeTree
ORDER BY id;


CREATE TABLE IF NOT EXISTS snowflake.supplier_dds
(
    id UInt64,
    supplier_name Nullable(String),
    supplier_contact Nullable(String),
    supplier_email Nullable(String),
    supplier_phone Nullable(String),
    supplier_address Nullable(String),
    supplier_city Nullable(String),
    supplier_country Nullable(String)
)
ENGINE = MergeTree
ORDER BY id;


CREATE TABLE IF NOT EXISTS snowflake.product_dds
(
    id UInt64,
    seller_id UInt64,
    store_id UInt64,
    supplier_id UInt64,

    product_name Nullable(String),
    product_category Nullable(String),
    product_price Nullable(Decimal(10,2)),
    product_quantity Nullable(Int32),
    pet_category Nullable(String),

    product_weight Nullable(Decimal(10,2)),
    product_color Nullable(String),
    product_size Nullable(String),
    product_brand Nullable(String),
    product_material Nullable(String),
    product_description Nullable(String),
    product_rating Nullable(Decimal(4,2)),
    product_reviews Nullable(Int32),
    product_release_date Nullable(String),
    product_expiry_date Nullable(String)
)
ENGINE = MergeTree
ORDER BY id;


CREATE TABLE IF NOT EXISTS snowflake.sale_dds
(
    id UInt64,
    customer_id UInt64,
    product_id UInt64,
    sale_date Nullable(String),
    sale_quantity Nullable(Int32),
    sale_total_price Nullable(Decimal(12,2))
)
ENGINE = MergeTree
ORDER BY id;


/* =================================================
 / REPORTS
================================================= */

-- ================== Product sales ==================
CREATE TABLE IF NOT EXISTS reports.product_sales_top10 (
    product_name String,
    total_sold UInt64
) ENGINE = MergeTree()
ORDER BY product_name;

CREATE TABLE IF NOT EXISTS reports.product_category_full (
    product_category String,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY product_category;

CREATE TABLE IF NOT EXISTS reports.product_sales_in_category (
    product_category String,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY product_category;

-- ================== Customer sales ==================
CREATE TABLE IF NOT EXISTS reports.customer_top10 (
    customer_first_name String,
    customer_last_name String,
    total_spent Float64
) ENGINE = MergeTree()
ORDER BY (customer_first_name, customer_last_name);

CREATE TABLE IF NOT EXISTS reports.customer_country (
    customer_country String,
    customer_count UInt64
) ENGINE = MergeTree()
ORDER BY customer_country;

CREATE TABLE IF NOT EXISTS reports.customer_cheque (
    customer_first_name String,
    customer_last_name String,
    avg_order_value Float64
) ENGINE = MergeTree()
ORDER BY (customer_first_name, customer_last_name);

-- ================== Time-based sales ==================
CREATE TABLE IF NOT EXISTS reports.month_time_based (
    sale_month String,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY sale_month;

CREATE TABLE IF NOT EXISTS reports.year_time_based (
    sale_year String,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY sale_year;

CREATE TABLE IF NOT EXISTS reports.revenue_comparison_for_different_periods (
    sale_year String,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY sale_year;

CREATE TABLE IF NOT EXISTS reports.average_order_size_by_month (
    sale_month String,
    avg_order_value Float64
) ENGINE = MergeTree()
ORDER BY sale_month;

-- ================== Store sales ==================
CREATE TABLE IF NOT EXISTS reports.top5_stores_revenue (
    store_name String,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY store_name;

CREATE TABLE IF NOT EXISTS reports.store_revenue_by_location (
    store_city String,
    store_country String,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY (store_city, store_country);

CREATE TABLE IF NOT EXISTS reports.avg_check_per_store (
    store_name String,
    avg_order_value Float64
) ENGINE = MergeTree()
ORDER BY store_name;

-- ================== Supplier sales ==================
CREATE TABLE IF NOT EXISTS reports.top5_suppliers_revenue (
    supplier_name String,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY supplier_name;

CREATE TABLE IF NOT EXISTS reports.avg_product_price_per_supplier (
    supplier_name String,
    avg_product_price Float64
) ENGINE = MergeTree()
ORDER BY supplier_name;

CREATE TABLE IF NOT EXISTS reports.sales_by_supplier_country (
    supplier_country String,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY supplier_country;

-- ================== Product ratings ==================
CREATE TABLE IF NOT EXISTS reports.top_rated_products (
    product_name String,
    product_rating Float64
) ENGINE = MergeTree()
ORDER BY product_name;

CREATE TABLE IF NOT EXISTS reports.lowest_rated_products (
    product_name String,
    product_rating Float64
) ENGINE = MergeTree()
ORDER BY product_name;

CREATE TABLE IF NOT EXISTS reports.products_by_reviews (
    product_name String,
    product_reviews UInt64
) ENGINE = MergeTree()
ORDER BY product_name;

CREATE TABLE IF NOT EXISTS reports.rating_sales_correlation (
    rating_sales_correlation Float64
) ENGINE = MergeTree()
ORDER BY rating_sales_correlation;

