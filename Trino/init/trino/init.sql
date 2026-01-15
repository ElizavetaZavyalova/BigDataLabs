--MAKE STAR
INSERT INTO clickhouse_target.star.customer_ods
              SELECT
                  row_number() OVER () AS id,
                  customer_first_name,
                  customer_last_name,
                  customer_age,
                  customer_email,
                  customer_country,
                  customer_postal_code,
                  customer_pet_type,
                  customer_pet_name,
                  customer_pet_breed
              FROM (
                  SELECT DISTINCT
                      customer_first_name,
                      customer_last_name,
                      customer_age,
                      customer_email,
                      customer_country,
                      customer_postal_code,
                      customer_pet_type,
                      customer_pet_name,
                      customer_pet_breed
                  FROM postgresql.public.mock_data

                  UNION ALL

                  SELECT DISTINCT
                      customer_first_name,
                      customer_last_name,
                      customer_age,
                      customer_email,
                      customer_country,
                      customer_postal_code,
                      customer_pet_type,
                      customer_pet_name,
                      customer_pet_breed
                  FROM clickhouse_source.default.mock_data
              ) combined;



INSERT INTO clickhouse_target.star.product_ods
SELECT
    row_number() OVER () AS id,
    seller_first_name,
    seller_last_name,
    seller_email,
    seller_country,
    seller_postal_code,
    product_name,
    product_category,
    product_price,
    product_quantity,
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email,
    pet_category,
    product_weight,
    product_color,
    product_size,
    product_brand,
    product_material,
    product_description,
    product_rating,
    product_reviews,
    product_release_date,
    product_expiry_date,
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
FROM (
    SELECT DISTINCT
        seller_first_name,
        seller_last_name,
        seller_email,
        seller_country,
        seller_postal_code,
        product_name,
        product_category,
        product_price,
        product_quantity,
        store_name,
        store_location,
        store_city,
        store_state,
        store_country,
        store_phone,
        store_email,
        pet_category,
        product_weight,
        product_color,
        product_size,
        product_brand,
        product_material,
        product_description,
        product_rating,
        product_reviews,
        product_release_date,
        product_expiry_date,
        supplier_name,
        supplier_contact,
        supplier_email,
        supplier_phone,
        supplier_address,
        supplier_city,
        supplier_country
    FROM postgresql.public.mock_data

    UNION ALL

    SELECT DISTINCT
        seller_first_name,
        seller_last_name,
        seller_email,
        seller_country,
        seller_postal_code,
        product_name,
        product_category,
        product_price,
        product_quantity,
        store_name,
        store_location,
        store_city,
        store_state,
        store_country,
        store_phone,
        store_email,
        pet_category,
        product_weight,
        product_color,
        product_size,
        product_brand,
        product_material,
        product_description,
        product_rating,
        product_reviews,
        product_release_date,
        product_expiry_date,
        supplier_name,
        supplier_contact,
        supplier_email,
        supplier_phone,
        supplier_address,
        supplier_city,
        supplier_country
    FROM clickhouse_source.default.mock_data
) combined;

INSERT INTO clickhouse_target.star.sale_ods
WITH unique_customers AS (
    SELECT id, customer_email
    FROM (
        SELECT id, customer_email,
               row_number() OVER (PARTITION BY customer_email ORDER BY id) AS rn
        FROM clickhouse_target.star.customer_ods
    ) t
    WHERE rn = 1
),
unique_products AS (
    SELECT id, product_name
    FROM (
        SELECT id, product_name,
               row_number() OVER (PARTITION BY product_name ORDER BY id) AS rn
        FROM clickhouse_target.star.product_ods
    ) t
    WHERE rn = 1
),
all_sales AS (
    SELECT * FROM postgresql.public.mock_data
    UNION ALL
    SELECT * FROM clickhouse_source.default.mock_data
)
SELECT
    row_number() OVER () AS id,
    c.id AS customer_id,
    p.id AS product_id,
    m.sale_date,
    m.sale_quantity,
    m.sale_total_price
FROM all_sales m
LEFT JOIN unique_customers c
    ON c.customer_email = m.customer_email
LEFT JOIN unique_products p
    ON p.product_name = m.product_name;

--MAKE SNOWFLAKE
INSERT INTO clickhouse_target.snowflake.pet_dds
SELECT id, customer_pet_type, customer_pet_name, customer_pet_breed
FROM clickhouse_target.star.customer_ods;

INSERT INTO clickhouse_target.snowflake.customer_dds
SELECT DISTINCT id, customer_first_name, customer_last_name, customer_age,
                customer_email, customer_country, customer_postal_code, id AS pet_id
FROM clickhouse_target.star.customer_ods;

INSERT INTO clickhouse_target.snowflake.seller_dds
SELECT DISTINCT id, seller_first_name, seller_last_name, seller_email,
                seller_country, seller_postal_code
FROM clickhouse_target.star.product_ods;

INSERT INTO clickhouse_target.snowflake.store_dds
SELECT DISTINCT id, store_name, store_location, store_city, store_state,
                store_country, store_phone, store_email
FROM clickhouse_target.star.product_ods;

INSERT INTO clickhouse_target.snowflake.supplier_dds
SELECT DISTINCT id, supplier_name, supplier_contact, supplier_email,
                supplier_phone, supplier_address, supplier_city, supplier_country
FROM clickhouse_target.star.product_ods;

INSERT INTO clickhouse_target.snowflake.product_dds
WITH unique_sellers AS (
    SELECT id, seller_email
    FROM (
        SELECT id, seller_email,
               row_number() OVER (PARTITION BY seller_email ORDER BY id) AS rn
        FROM clickhouse_target.snowflake.seller_dds
    ) t
    WHERE rn = 1
),
unique_stores AS (
    SELECT id, store_email
    FROM (
        SELECT id, store_email,
               row_number() OVER (PARTITION BY store_email ORDER BY id) AS rn
        FROM clickhouse_target.snowflake.store_dds
    ) t
    WHERE rn = 1
),
unique_suppliers AS (
    SELECT id, supplier_email
    FROM (
        SELECT id, supplier_email,
               row_number() OVER (PARTITION BY supplier_email ORDER BY id) AS rn
        FROM clickhouse_target.snowflake.supplier_dds
    ) t
    WHERE rn = 1
)
SELECT
    m.id,
    s.id AS seller_id,
    st.id AS store_id,
    sup.id AS supplier_id,
    m.product_name,
    m.product_category,
    m.product_price,
    m.product_quantity,
    m.pet_category,
    m.product_weight,
    m.product_color,
    m.product_size,
    m.product_brand,
    m.product_material,
    m.product_description,
    m.product_rating,
    m.product_reviews,
    m.product_release_date,
    m.product_expiry_date
FROM clickhouse_target.star.product_ods m
LEFT JOIN unique_sellers s
    ON s.seller_email = m.seller_email
LEFT JOIN unique_stores st
    ON st.store_email = m.store_email
LEFT JOIN unique_suppliers sup
    ON sup.supplier_email = m.supplier_email;

INSERT INTO clickhouse_target.snowflake.sale_dds
SELECT DISTINCT id, customer_id, product_id, sale_date,
                sale_quantity, sale_total_price
FROM clickhouse_target.star.sale_ods;

--MAKE REPORTS
--================== Product sales ==================
INSERT INTO clickhouse_target.reports.product_sales_top10
SELECT
    p.product_name,
    SUM(s.sale_quantity) AS total_sold
FROM clickhouse_target.snowflake.sale_dds s
JOIN clickhouse_target.snowflake.product_dds p
    ON s.product_id = p.id
GROUP BY p.product_name
ORDER BY total_sold DESC
LIMIT 10;

INSERT INTO clickhouse_target.reports.product_category_full
SELECT
    p.product_category,
    SUM(s.sale_total_price) AS total_revenue
FROM clickhouse_target.snowflake.sale_dds s
JOIN clickhouse_target.snowflake.product_dds p
    ON s.product_id = p.id
GROUP BY p.product_category
ORDER BY total_revenue DESC;

INSERT INTO clickhouse_target.reports.product_sales_in_category
SELECT
    p.product_category,
    SUM(s.sale_total_price) AS total_revenue
FROM clickhouse_target.snowflake.sale_dds s
JOIN clickhouse_target.snowflake.product_dds p
    ON s.product_id = p.id
GROUP BY p.product_category
ORDER BY total_revenue DESC;
--================== Customer sales ==================
INSERT INTO clickhouse_target.reports.customer_top10
SELECT
    c.customer_first_name,
    c.customer_last_name,
    SUM(s.sale_total_price) AS total_spent
FROM clickhouse_target.snowflake.sale_dds s
JOIN clickhouse_target.snowflake.customer_dds c
    ON s.customer_id = c.id
GROUP BY
    c.id,
    c.customer_first_name,
    c.customer_last_name
ORDER BY total_spent DESC
LIMIT 10;

INSERT INTO clickhouse_target.reports.customer_cheque
SELECT
    c.customer_first_name,
    c.customer_last_name,
    AVG(s.sale_total_price) AS avg_order_value
FROM clickhouse_target.snowflake.sale_dds s
JOIN clickhouse_target.snowflake.customer_dds c
    ON s.customer_id = c.id
GROUP BY
    c.id,
    c.customer_first_name,
    c.customer_last_name
ORDER BY avg_order_value DESC;

-- ================== Time-based sales ==================
INSERT INTO clickhouse_target.reports.month_time_based
SELECT
    format_datetime(date_parse(s.sale_date, '%m/%d/%Y'), 'yyyy-MM') AS sale_month,
    SUM(s.sale_total_price) AS total_revenue
FROM clickhouse_target.snowflake.sale_dds s
GROUP BY format_datetime(date_parse(s.sale_date, '%m/%d/%Y'), 'yyyy-MM')
ORDER BY format_datetime(date_parse(s.sale_date, '%m/%d/%Y'), 'yyyy-MM');

INSERT INTO clickhouse_target.reports.year_time_based
SELECT
    format_datetime(date_parse(s.sale_date, '%m/%d/%Y'), 'yyyy') AS sale_year,
    SUM(s.sale_total_price) AS total_revenue
FROM clickhouse_target.snowflake.sale_dds s
GROUP BY format_datetime(date_parse(s.sale_date, '%m/%d/%Y'), 'yyyy')
ORDER BY format_datetime(date_parse(s.sale_date, '%m/%d/%Y'), 'yyyy');

INSERT INTO clickhouse_target.reports.revenue_comparison_for_different_periods
SELECT
    CAST(year(date_parse(s.sale_date, '%m/%d/%Y')) AS VARCHAR) AS sale_year,
    SUM(s.sale_total_price) AS total_revenue
FROM clickhouse_target.snowflake.sale_dds s
GROUP BY year(date_parse(s.sale_date, '%m/%d/%Y'))
ORDER BY year(date_parse(s.sale_date, '%m/%d/%Y'));

INSERT INTO clickhouse_target.reports.average_order_size_by_month
WITH month_sales AS (
    SELECT
        format_datetime(date_parse(s.sale_date, '%m/%d/%Y'), 'yyyy-MM') AS sale_month,
        s.sale_total_price
    FROM clickhouse_target.snowflake.sale_dds s
)
SELECT sale_month, SUM(sale_total_price) AS total_revenue
FROM month_sales
GROUP BY sale_month
ORDER BY sale_month;


-- ================== Store sales ==================
INSERT INTO clickhouse_target.reports.top5_stores_revenue
SELECT
    st.store_name,
    SUM(sa.sale_total_price) AS total_revenue
FROM clickhouse_target.snowflake.sale_dds sa
JOIN clickhouse_target.snowflake.product_dds p
    ON sa.product_id = p.id
JOIN clickhouse_target.snowflake.store_dds st
    ON p.store_id = st.id
GROUP BY st.store_name
ORDER BY total_revenue DESC
LIMIT 5;

INSERT INTO clickhouse_target.reports.store_revenue_by_location
SELECT
    st.store_city,
    st.store_country,
    SUM(sa.sale_total_price) AS total_revenue
FROM clickhouse_target.snowflake.sale_dds sa
JOIN clickhouse_target.snowflake.product_dds p
    ON sa.product_id = p.id
JOIN clickhouse_target.snowflake.store_dds st
    ON p.store_id = st.id
GROUP BY st.store_city, st.store_country
ORDER BY total_revenue DESC;

INSERT INTO clickhouse_target.reports.avg_check_per_store
SELECT
    st.store_name,
    AVG(sa.sale_total_price) AS avg_order_value
FROM clickhouse_target.snowflake.sale_dds sa
JOIN clickhouse_target.snowflake.product_dds p
    ON sa.product_id = p.id
JOIN clickhouse_target.snowflake.store_dds st
    ON p.store_id = st.id
GROUP BY st.store_name
ORDER BY avg_order_value DESC;

-- ================== Supplier sales ==================
INSERT INTO clickhouse_target.reports.top5_suppliers_revenue
SELECT
    sup.supplier_name,
    SUM(sa.sale_total_price) AS total_revenue
FROM clickhouse_target.snowflake.sale_dds sa
JOIN clickhouse_target.snowflake.product_dds p
    ON sa.product_id = p.id
JOIN clickhouse_target.snowflake.supplier_dds sup
    ON p.supplier_id = sup.id
GROUP BY sup.supplier_name
ORDER BY total_revenue DESC
LIMIT 5;

INSERT INTO clickhouse_target.reports.avg_product_price_per_supplier
SELECT
    sup.supplier_name,
    AVG(p.product_price) AS avg_product_price
FROM clickhouse_target.snowflake.product_dds p
JOIN clickhouse_target.snowflake.supplier_dds sup
    ON p.supplier_id = sup.id
GROUP BY sup.supplier_name
ORDER BY avg_product_price DESC;

INSERT INTO clickhouse_target.reports.sales_by_supplier_country
SELECT
    sup.supplier_country,
    SUM(sa.sale_total_price) AS total_revenue
FROM clickhouse_target.snowflake.sale_dds sa
JOIN clickhouse_target.snowflake.product_dds p
    ON sa.product_id = p.id
JOIN clickhouse_target.snowflake.supplier_dds sup
    ON p.supplier_id = sup.id
GROUP BY sup.supplier_country
ORDER BY total_revenue DESC;
-- ================== Product ratings ==================

INSERT INTO clickhouse_target.reports.top_rated_products
SELECT
    product_name,
    product_rating
FROM clickhouse_target.snowflake.product_dds
ORDER BY product_rating DESC
LIMIT 5;

INSERT INTO clickhouse_target.reports.lowest_rated_products
SELECT
    product_name,
    product_rating
FROM clickhouse_target.snowflake.product_dds
ORDER BY product_rating ASC
LIMIT 5;

INSERT INTO clickhouse_target.reports.products_by_reviews
SELECT
    product_name,
    product_reviews
FROM clickhouse_target.snowflake.product_dds
ORDER BY product_reviews DESC
LIMIT 10;

INSERT INTO clickhouse_target.reports.rating_sales_correlation
SELECT
    corr(p.product_rating, sa.sale_quantity) AS rating_sales_correlation
FROM clickhouse_target.snowflake.sale_dds sa
JOIN clickhouse_target.snowflake.product_dds p
    ON sa.product_id = p.id;