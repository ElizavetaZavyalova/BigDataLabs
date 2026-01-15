CREATE TABLE IF NOT EXISTS default.mock_data
(
    id                   Int32,
    customer_first_name  String,
    customer_last_name   String,
    customer_age         Int32,
    customer_email       String,
    customer_country     String,
    customer_postal_code String,
    customer_pet_type    String,
    customer_pet_name    String,
    customer_pet_breed   String,

    seller_first_name    String,
    seller_last_name     String,
    seller_email         String,
    seller_country       String,
    seller_postal_code   String,

    product_name         String,
    product_category     String,
    product_price        Decimal(10, 2),
    product_quantity     Int32,

    sale_date            String,
    sale_customer_id     Int32,
    sale_seller_id       Int32,
    sale_product_id      Int32,
    sale_quantity        Int32,
    sale_total_price     Decimal(12, 2),

    store_name           String,
    store_location       String,
    store_city           String,
    store_state          String,
    store_country        String,
    store_phone          String,
    store_email          String,
    pet_category         String,

    product_weight       Decimal(10, 2),
    product_color        String,
    product_size         String,
    product_brand        String,
    product_material     String,
    product_description  String,
    product_rating       Decimal(4, 2),
    product_reviews      Int32,
    product_release_date String,
    product_expiry_date  String,

    supplier_name        String,
    supplier_contact     String,
    supplier_email       String,
    supplier_phone       String,
    supplier_address     String,
    supplier_city        String,
    supplier_country     String
)
ENGINE = MergeTree()
ORDER BY id;
INSERT INTO default.mock_data
FROM INFILE '/data/clickhouse/MOCK_DATA_6.csv'
FORMAT CSVWithNames;
INSERT INTO default.mock_data
FROM INFILE '/data/clickhouse/MOCK_DATA_7.csv'
FORMAT CSVWithNames;
INSERT INTO default.mock_data
FROM INFILE '/data/clickhouse/MOCK_DATA_8.csv'
FORMAT CSVWithNames;
INSERT INTO default.mock_data
FROM INFILE '/data/clickhouse/MOCK_DATA_9.csv'
FORMAT CSVWithNames;
INSERT INTO default.mock_data
FROM INFILE '/data/clickhouse/MOCK_DATA_10.csv'
FORMAT CSVWithNames;