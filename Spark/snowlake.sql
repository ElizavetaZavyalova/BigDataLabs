create schema ods;
create schema data_mart;
create schema dds;
CREATE TABLE lab1.ods.mock_data
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

    sale_date            DATE,
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
    product_release_date DATE,
    product_expiry_date  DATE,

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
CREATE TABLE lab1.dds.customer_ods
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

INSERT INTO lab1.dds.customer_ods (customer_first_name,
                                   customer_last_name,
                                   customer_age,
                                   customer_email,
                                   customer_country,
                                   customer_postal_code,
                                   customer_pet_type,
                                   customer_pet_name,
                                   customer_pet_breed)
select DISTINCT customer_first_name,
                customer_last_name,
                customer_age,
                customer_email,
                customer_country,
                customer_postal_code,
                customer_pet_type,
                customer_pet_name,
                customer_pet_breed
FROM lab1.ods.mock_data;

CREATE TABLE lab1.dds.product_ods
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
    product_release_date DATE,
    product_expiry_date  DATE,

    supplier_name        TEXT,
    supplier_contact     TEXT,
    supplier_email       TEXT,
    supplier_phone       TEXT,
    supplier_address     TEXT,
    supplier_city        TEXT,
    supplier_country     TEXT
);

INSERT INTO lab1.dds.product_ods (seller_first_name,
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
                                  supplier_country)
SELECT DISTINCT seller_first_name,
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
FROM lab1.ods.mock_data;


CREATE TABLE lab1.dds.sale_ods
(
    id               SERIAL primary key,
    customer_id      INT,
    product_id       INT,
    sale_date        DATE,
    sale_quantity    INT,
    sale_total_price NUMERIC(12, 2),
    CONSTRAINT fk_customer
        FOREIGN KEY (customer_id) REFERENCES lab1.dds.customer_ods (id),
    CONSTRAINT fk_product
        FOREIGN KEY (product_id) REFERENCES lab1.dds.product_ods (id)
);
INSERT INTO lab1.dds.sale_ods (customer_id,
                               product_id,
                               sale_date,
                               sale_quantity,
                               sale_total_price)
SELECT DISTINCT (SELECT c.id
                 FROM lab1.dds.customer_ods c
                 WHERE c.customer_email = m.customer_email
                 LIMIT 1) AS customer_id,

                (SELECT p.id
                 FROM lab1.dds.product_ods p
                 WHERE p.product_name = m.product_name
                 LIMIT 1) AS product_id,

                m.sale_date,
                m.sale_quantity,
                m.sale_total_price
FROM lab1.ods.mock_data m;
-------------------------------------------------
--до снежинки
------------------------------------------------
CREATE TABLE lab1.data_mart.pet_dds
(
    id                 SERIAL primary key,
    customer_pet_type  TEXT,
    customer_pet_name  TEXT,
    customer_pet_breed TEXT
);
INSERT INTO lab1.data_mart.pet_dds (id,
                                    customer_pet_type,
                                    customer_pet_name,
                                    customer_pet_breed)
select id,
       customer_pet_type,
       customer_pet_name,
       customer_pet_breed
FROM lab1.dds.customer_ods;

CREATE TABLE lab1.data_mart.customer_dds
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
        FOREIGN KEY (pet_id) REFERENCES lab1.data_mart.pet_dds (id)
);
INSERT INTO lab1.data_mart.customer_dds (id,
                                         customer_first_name,
                                         customer_last_name,
                                         customer_age,
                                         customer_email,
                                         customer_country,
                                         customer_postal_code,
                                         pet_id)
SELECT distinct id,
                m.customer_first_name,
                m.customer_last_name,
                m.customer_age,
                m.customer_email,
                m.customer_country,
                m.customer_postal_code,
                m.id AS pet_id
FROM lab1.dds.customer_ods m

CREATE TABLE lab1.data_mart.seller_dds
(
    id                 SERIAL primary key,
    seller_first_name  TEXT,
    seller_last_name   TEXT,
    seller_email       TEXT,
    seller_country     TEXT,
    seller_postal_code text
);

INSERT INTO lab1.data_mart.seller_dds (id,
                                       seller_first_name,
                                       seller_last_name,
                                       seller_email,
                                       seller_country,
                                       seller_postal_code)
SELECT distinct id,
                seller_first_name,
                seller_last_name,
                seller_email,
                seller_country,
                seller_postal_code
FROM lab1.dds.product_ods CREATE TABLE lab1.data_mart.store_dds (
    id SERIAL primary key,
    store_name TEXT,
    store_location TEXT,
    store_city TEXT,
    store_state TEXT,
    store_country TEXT,
    store_phone TEXT,
    store_email text);

INSERT INTO lab1.data_mart.store_dds (id,
                                      store_name,
                                      store_location,
                                      store_city,
                                      store_state,
                                      store_country,
                                      store_phone,
                                      store_email)
SELECT distinct id,
                store_name,
                store_location,
                store_city,
                store_state,
                store_country,
                store_phone,
                store_email
FROM lab1.dds.product_ods CREATE TABLE lab1.data_mart.supplier_dds (
    id SERIAL primary key,
    supplier_name TEXT,
    supplier_contact TEXT,
    supplier_email TEXT,
    supplier_phone TEXT,
    supplier_address TEXT,
    supplier_city TEXT,
    supplier_country TEXT
    );

INSERT INTO lab1.data_mart.supplier_dds (id,
                                         supplier_name,
                                         supplier_contact,
                                         supplier_email,
                                         supplier_phone,
                                         supplier_address,
                                         supplier_city,
                                         supplier_country)
SELECT distinct id,
                supplier_name,
                supplier_contact,
                supplier_email,
                supplier_phone,
                supplier_address,
                supplier_city,
                supplier_country
FROM lab1.dds.product_ods CREATE TABLE lab1.data_mart.product_dds (
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
    product_release_date DATE,
    product_expiry_date DATE,
    CONSTRAINT fk_seller
        FOREIGN KEY (seller_id) REFERENCES lab1.data_mart.seller_dds(id),
    CONSTRAINT fk_store
        FOREIGN KEY (store_id) REFERENCES lab1.data_mart.store_dds(id),
    CONSTRAINT fk_supplier
        FOREIGN KEY (supplier_id) REFERENCES lab1.data_mart.supplier_dds(id)
);

INSERT INTO lab1.data_mart.product_dds (id,
                                        seller_id,
                                        store_id,
                                        supplier_id,
                                        product_name,
                                        product_category,
                                        product_price,
                                        product_quantity,
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
                                        product_expiry_date)
SELECT id,
       (SELECT id
        FROM lab1.data_mart.seller_dds c
        WHERE c.seller_email = m.seller_email
        LIMIT 1) AS customer_id,
       (SELECT id
        FROM lab1.data_mart.store_dds c
        WHERE c.store_email = m.store_email
        LIMIT 1) AS store_id,
       (SELECT id
        FROM lab1.data_mart.supplier_dds c
        WHERE c.supplier_email = m.supplier_email
        LIMIT 1) AS supplier_id,
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
FROM lab1.dds.product_ods m;

CREATE TABLE lab1.data_mart.sale_dds
(
    id               SERIAL primary key,
    customer_id      INT,
    product_id       INT,
    sale_date        DATE,
    sale_quantity    INT,
    sale_total_price NUMERIC(12, 2),
    CONSTRAINT fk_customer
        FOREIGN KEY (customer_id) REFERENCES lab1.data_mart.customer_dds (id),
    CONSTRAINT fk_product
        FOREIGN KEY (product_id) REFERENCES lab1.data_mart.product_dds (id)
);
INSERT INTO lab1.data_mart.sale_dds (id,
                                     customer_id,
                                     product_id,
                                     sale_date,
                                     sale_quantity,
                                     sale_total_price)
SELECT distinct m.id,
                m.customer_id,
                m.product_id,

                m.sale_date,
                m.sale_quantity,
                m.sale_total_price
FROM lab1.dds.sale_ods m;