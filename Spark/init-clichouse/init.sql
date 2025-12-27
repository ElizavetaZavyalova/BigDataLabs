-- ================== Витрины продаж по продуктам ==================

CREATE TABLE IF NOT EXISTS productSalesTop10 (
    product_name String,
    total_sold UInt64
) ENGINE = MergeTree()
ORDER BY product_name;

CREATE TABLE IF NOT EXISTS productCategoryFull (
    product_category String,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY product_category;

CREATE TABLE IF NOT EXISTS productSalesInCategory (
    product_category String,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY product_category;

-- ================== Витрины продаж по клиентам ==================

CREATE TABLE IF NOT EXISTS customerTop10 (
    customer_first_name String,
    customer_last_name String,
    total_spent Float64
) ENGINE = MergeTree()
ORDER BY (customer_first_name, customer_last_name);

CREATE TABLE IF NOT EXISTS customerCountry (
    customer_country String,
    customer_count UInt64
) ENGINE = MergeTree()
ORDER BY customer_country;

CREATE TABLE IF NOT EXISTS customerCheque (
    customer_first_name String,
    customer_last_name String,
    avg_order_value Float64
) ENGINE = MergeTree()
ORDER BY (customer_first_name, customer_last_name);

-- ================== Витрины продаж по времени ==================

CREATE TABLE IF NOT EXISTS monthTimeBased (
    sale_month String,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY sale_month;

CREATE TABLE IF NOT EXISTS yearTimeBased (
    sale_year String,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY sale_year;

CREATE TABLE IF NOT EXISTS revenueComparisonForDifferentPeriods (
    sale_year String,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY sale_year;

CREATE TABLE IF NOT EXISTS averageOrderSizeByMonth (
    sale_month String,
    avg_order_value Float64
) ENGINE = MergeTree()
ORDER BY sale_month;

-- ================== Витрины продаж по магазинам ==================

CREATE TABLE IF NOT EXISTS top5StoresRevenue (
    store_name String,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY store_name;

CREATE TABLE IF NOT EXISTS storeRevenueByLocation (
    store_city String,
    store_country String,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY (store_city, store_country);

CREATE TABLE IF NOT EXISTS avgCheckPerStore (
    store_name String,
    avg_order_value Float64
) ENGINE = MergeTree()
ORDER BY store_name;

-- ================== Витрины продаж по поставщикам ==================

CREATE TABLE IF NOT EXISTS top5SuppliersRevenue (
    supplier_name String,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY supplier_name;

CREATE TABLE IF NOT EXISTS avgProductPricePerSupplier (
    supplier_name String,
    avg_product_price Float64
) ENGINE = MergeTree()
ORDER BY supplier_name;

CREATE TABLE IF NOT EXISTS salesBySupplierCountry (
    supplier_country String,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY supplier_country;

-- ================== Витрины рейтингов продуктов ==================

CREATE TABLE IF NOT EXISTS topRatedProducts (
    product_name String,
    product_rating Float64
) ENGINE = MergeTree()
ORDER BY product_name;

CREATE TABLE IF NOT EXISTS lowestRatedProducts (
    product_name String,
    product_rating Float64
) ENGINE = MergeTree()
ORDER BY product_name;

CREATE TABLE IF NOT EXISTS productsByReviews (
    product_name String,
    product_reviews UInt64
) ENGINE = MergeTree()
ORDER BY product_name;

CREATE TABLE IF NOT EXISTS ratingSalesCorrelation (
    rating_sales_correlation Float64
) ENGINE = MergeTree()
ORDER BY rating_sales_correlation;
