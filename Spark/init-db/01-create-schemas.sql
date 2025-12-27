DROP TABLE IF EXISTS postgres.data_mart.sale_dds CASCADE;
DROP TABLE IF EXISTS postgres.data_mart.product_dds CASCADE;
DROP TABLE IF EXISTS postgres.data_mart.customer_dds CASCADE;
DROP TABLE IF EXISTS postgres.data_mart.pet_dds CASCADE;
DROP TABLE IF EXISTS postgres.data_mart.seller_dds CASCADE;
DROP TABLE IF EXISTS postgres.data_mart.store_dds CASCADE;
DROP TABLE IF EXISTS postgres.data_mart.supplier_dds CASCADE;

DROP TABLE IF EXISTS postgres.dds.sale_ods CASCADE;
DROP TABLE IF EXISTS postgres.dds.product_ods CASCADE;
DROP TABLE IF EXISTS postgres.dds.customer_ods CASCADE;

DROP TABLE IF EXISTS postgres.ods.mock_data CASCADE;

CREATE SCHEMA IF NOT EXISTS ods;
CREATE SCHEMA IF NOT EXISTS dds;
CREATE SCHEMA IF NOT EXISTS data_mart;
CREATE TABLE postgres.ods.mock_data
(
    id                   INT,
    customer_first_name  TEXT,
    customer_last_name   TEXT,
    customer_age         INT,
    customer_email       TEXT,
    customer_country     TEXT,
    customer_postal_code TEXT,
    customer_pet_type    TEXT,
    customer_pet_name    TEXT,
    customer_pet_breed   TEXT,

    seller_first_name    TEXT,
    seller_last_name     TEXT,
    seller_email         TEXT,
    seller_country       TEXT,
    seller_postal_code   TEXT,

    product_name         TEXT,
    product_category     TEXT,
    product_price        NUMERIC(10, 2),
    product_quantity     INT,

    sale_date            TEXT,
    sale_customer_id     INT,
    sale_seller_id       INT,
    sale_product_id      INT,
    sale_quantity        INT,
    sale_total_price     NUMERIC(12, 2),

    store_name           TEXT,
    store_location       TEXT,
    store_city           TEXT,
    store_state          TEXT,
    store_country        TEXT,
    store_phone          TEXT,
    store_email          TEXT,
    pet_category         TEXT,

    product_weight       NUMERIC(10, 2),
    product_color        TEXT,
    product_size         TEXT,
    product_brand        TEXT,
    product_material     TEXT,
    product_description  TEXT,
    product_rating       NUMERIC(4, 2),
    product_reviews      INT,
    product_release_date TEXT,
    product_expiry_date  TEXT,

    supplier_name        TEXT,
    supplier_contact     TEXT,
    supplier_email       TEXT,
    supplier_phone       TEXT,
    supplier_address     TEXT,
    supplier_city        TEXT,
    supplier_country     TEXT
);
-------------------------------------------------
--до звезды
------------------------------------------------
CREATE TABLE postgres.dds.customer_ods
(
    id                   SERIAL primary key,
    customer_first_name  TEXT,
    customer_last_name   TEXT,
    customer_age         INT,
    customer_email       TEXT,
    customer_country     TEXT,
    customer_postal_code TEXT,
    customer_pet_type    TEXT,
    customer_pet_name    TEXT,
    customer_pet_breed   TEXT
);


CREATE TABLE postgres.dds.product_ods
(
    id                   SERIAL primary key,
    seller_first_name    TEXT,
    seller_last_name     TEXT,
    seller_email         TEXT,
    seller_country       TEXT,
    seller_postal_code   TEXT,

    product_name         TEXT,
    product_category     TEXT,
    product_price        NUMERIC(10, 2),
    product_quantity     INT,

    store_name           TEXT,
    store_location       TEXT,
    store_city           TEXT,
    store_state          TEXT,
    store_country        TEXT,
    store_phone          TEXT,
    store_email          TEXT,
    pet_category         TEXT,

    product_weight       NUMERIC(10, 2),
    product_color        TEXT,
    product_size         TEXT,
    product_brand        TEXT,
    product_material     TEXT,
    product_description  TEXT,
    product_rating       NUMERIC(4, 2),
    product_reviews      INT,
    product_release_date TEXT,
    product_expiry_date  TEXT,

    supplier_name        TEXT,
    supplier_contact     TEXT,
    supplier_email       TEXT,
    supplier_phone       TEXT,
    supplier_address     TEXT,
    supplier_city        TEXT,
    supplier_country     TEXT
);


CREATE TABLE postgres.dds.sale_ods
(
    id               SERIAL primary key,
    customer_id      INT,
    product_id       INT,
    sale_date        TEXT,
    sale_quantity    INT,
    sale_total_price NUMERIC(12, 2),
    CONSTRAINT fk_customer
        FOREIGN KEY (customer_id) REFERENCES postgres.dds.customer_ods (id),
    CONSTRAINT fk_product
        FOREIGN KEY (product_id) REFERENCES postgres.dds.product_ods (id)
);
-------------------------------------------------
--до снежинки
------------------------------------------------
CREATE TABLE postgres.data_mart.pet_dds
(
    id                 SERIAL primary key,
    customer_pet_type  TEXT,
    customer_pet_name  TEXT,
    customer_pet_breed TEXT
);

CREATE TABLE postgres.data_mart.customer_dds
(
    id                   SERIAL primary key,
    customer_first_name  TEXT,
    customer_last_name   TEXT,
    customer_age         INT,
    customer_email       TEXT,
    customer_country     TEXT,
    customer_postal_code TEXT,
    pet_id               INT,
    CONSTRAINT fk_pet
        FOREIGN KEY (pet_id) REFERENCES postgres.data_mart.pet_dds (id)
);

CREATE TABLE postgres.data_mart.seller_dds
(
    id                 SERIAL primary key,
    seller_first_name  TEXT,
    seller_last_name   TEXT,
    seller_email       TEXT,
    seller_country     TEXT,
    seller_postal_code text
);

CREATE TABLE postgres.data_mart.store_dds (
    id SERIAL primary key, 
    store_name TEXT,                            
    store_location TEXT,                        
    store_city TEXT,                            
    store_state TEXT,                           
    store_country TEXT,                         
    store_phone TEXT,                           
    store_email text);
CREATE TABLE postgres.data_mart.supplier_dds (
                                             id SERIAL primary key,
                                             supplier_name TEXT,
                                             supplier_contact TEXT,
                                             supplier_email TEXT,
                                             supplier_phone TEXT,
                                             supplier_address TEXT,
                                             supplier_city TEXT,
                                             supplier_country TEXT
);

 CREATE TABLE postgres.data_mart.product_dds (
    id SERIAL primary key, 
    seller_id int,
    store_id INT,
    supplier_id INT,
    
    product_name TEXT,                          
    product_category TEXT,                      
    product_price NUMERIC(10,2),               
    product_quantity INT,                       
    pet_category TEXT,                          
    
    product_weight NUMERIC(10,2),               
    product_color TEXT,                         
    product_size TEXT,                           
    product_brand TEXT,                         
    product_material TEXT,                      
    product_description TEXT,                   
    product_rating NUMERIC(4,2),                
    product_reviews INT,                        
    product_release_date TEXT,                  
    product_expiry_date TEXT,                   
    CONSTRAINT fk_seller
        FOREIGN KEY (seller_id) REFERENCES postgres.data_mart.seller_dds(id),
    CONSTRAINT fk_store
        FOREIGN KEY (store_id) REFERENCES postgres.data_mart.store_dds(id),
    CONSTRAINT fk_supplier
        FOREIGN KEY (supplier_id) REFERENCES postgres.data_mart.supplier_dds(id)
);

CREATE TABLE postgres.data_mart.sale_dds
(
    id              SERIAL primary key,
    customer_id      INT,
    product_id       INT,
    sale_date        TEXT,
    sale_quantity    INT,
    sale_total_price NUMERIC(12, 2),
    CONSTRAINT fk_customer
        FOREIGN KEY (customer_id) REFERENCES postgres.data_mart.customer_dds (id),
    CONSTRAINT fk_product
        FOREIGN KEY (product_id) REFERENCES postgres.data_mart.product_dds (id)
);
