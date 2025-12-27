package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class SparkSnowflakeApp {

    Dataset<Row> readFromPostgres(SparkSession spark, String sql) {
        String jdbcUrl = "jdbc:postgresql://postgres:5432/postgres";
        return spark.read()
                .format("jdbc")
                .option("url", jdbcUrl)
                .option("dbtable", "( " + sql + " ) AS t")
                .option("user", "postgres")
                .option("password", "postgres")
                .option("driver", "org.postgresql.Driver")
                .load();
    }

    Dataset<Row> readFromCsv(SparkSession spark) {
        return spark.read()
                .option("header", "true")
                .option("multiLine", "true")
                .option("quote", "\"")
                .option("escape", "\"")
                .option("sep", ",")
                .option("inferSchema", "true")
                .option("nullValue", "")
                .option("encoding", "UTF-8")
                .csv("data/*.csv");
    }

    public void writeToClickhouse(Dataset<Row> data, String tableName) {
        String jdbcUrl = "jdbc:clickhouse://clickhouse:8123/default?socket_timeout=300000&compress=0";

        data.write()
                .format("jdbc")
                .option("url", jdbcUrl)
                .option("dbtable", tableName)
                .option("user", "default")
                .option("password", "pass")
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
                .option("batchsize", "10000")
                .option("isolationLevel", "NONE")
                .mode(SaveMode.Append)
                .save();
    }

    void writeToPostgres(Dataset<Row> data, String name) {
        String jdbcUrl = "jdbc:postgresql://postgres:5432/postgres";
        Properties props = new Properties();
        props.put("user", "postgres");
        props.put("password", "postgres");
        props.put("driver", "org.postgresql.Driver");
        data.write().mode("append").jdbc(jdbcUrl, name, props);
    }

    public void runSparkLoad() {

        SparkSession spark = SparkSession.builder()
                .appName("ETL")
                .master("local[*]")
                .getOrCreate();

        writeToPostgres(readFromCsv(spark), "ods.mock_data");

       writeToPostgres(readFromPostgres(spark,
                "SELECT DISTINCT customer_first_name, customer_last_name, customer_age, customer_email, " +
                        "customer_country, customer_postal_code, customer_pet_type, customer_pet_name, customer_pet_breed " +
                        "FROM ods.mock_data"), "dds.customer_ods");

        writeToPostgres(readFromPostgres(spark,
                "SELECT DISTINCT seller_first_name, seller_last_name, seller_email, seller_country, seller_postal_code, " +
                        "product_name, product_category, product_price, product_quantity, " +
                        "store_name, store_location, store_city, store_state, store_country, store_phone, store_email, pet_category, " +
                        "product_weight, product_color, product_size, product_brand, product_material, product_description, product_rating, product_reviews, " +
                        "product_release_date, product_expiry_date, supplier_name, supplier_contact, supplier_email, supplier_phone, supplier_address, supplier_city, supplier_country " +
                        "FROM ods.mock_data"), "dds.product_ods");

        writeToPostgres(readFromPostgres(spark,
                "SELECT DISTINCT " +
                        "(SELECT id FROM dds.customer_ods c WHERE c.customer_email = m.customer_email LIMIT 1) AS customer_id, " +
                        "(SELECT id FROM dds.product_ods p WHERE p.product_name = m.product_name LIMIT 1) AS product_id, " +
                        "m.sale_date, m.sale_quantity, m.sale_total_price " +
                        "FROM ods.mock_data m"), "dds.sale_ods");

        writeToPostgres(readFromPostgres(spark,
                "SELECT id, customer_pet_type, customer_pet_name, customer_pet_breed FROM dds.customer_ods"), "data_mart.pet_dds");

        writeToPostgres(readFromPostgres(spark,
                "SELECT DISTINCT id, customer_first_name, customer_last_name, customer_age, customer_email, customer_country, customer_postal_code, id AS pet_id " +
                        "FROM dds.customer_ods"), "data_mart.customer_dds");

        writeToPostgres(readFromPostgres(spark,
                "SELECT DISTINCT id, seller_first_name, seller_last_name, seller_email, seller_country, seller_postal_code FROM dds.product_ods"), "data_mart.seller_dds");


        writeToPostgres(readFromPostgres(spark,
                "SELECT DISTINCT id, store_name, store_location, store_city, store_state, store_country, store_phone, store_email FROM dds.product_ods"), "data_mart.store_dds");


        writeToPostgres(readFromPostgres(spark,
                "SELECT DISTINCT id, supplier_name, supplier_contact, supplier_email, supplier_phone, supplier_address, supplier_city, supplier_country FROM dds.product_ods"), "data_mart.supplier_dds");

        writeToPostgres(readFromPostgres(spark,
                "SELECT id, " +
                        "(SELECT id FROM data_mart.seller_dds c WHERE c.seller_email = m.seller_email LIMIT 1) AS seller_id, " +
                        "(SELECT id FROM data_mart.store_dds c WHERE c.store_email = m.store_email LIMIT 1) AS store_id, " +
                        "(SELECT id FROM data_mart.supplier_dds c WHERE c.supplier_email = m.supplier_email LIMIT 1) AS supplier_id, " +
                        "product_name, product_category, product_price, product_quantity, pet_category, product_weight, product_color, product_size, product_brand, " +
                        "product_material, product_description, product_rating, product_reviews, product_release_date, product_expiry_date " +
                        "FROM dds.product_ods m"), "data_mart.product_dds");

        writeToPostgres(readFromPostgres(spark,
                "SELECT DISTINCT id, customer_id, product_id, sale_date, sale_quantity, sale_total_price FROM dds.sale_ods"), "data_mart.sale_dds");
        productSalesShowcase(spark);
        customerSalesShowcase(spark);
        timeBasedSalesShowcase(spark);
        storeSalesShowcase(spark);
        supplierSalesShowcase(spark);
        productRatingsShowcase(spark);
        spark.stop();
    }

    public void productSalesShowcase(SparkSession spark) {
        Dataset<Row> productSalesTop10 = readFromPostgres(spark,
                "SELECT \n" +
                        "    p.product_name,\n" +
                        "    SUM(s.sale_quantity) AS total_sold\n" +
                        "FROM \n" +
                        "    postgres.data_mart.sale_dds s\n" +
                        "JOIN \n" +
                        "    postgres.data_mart.product_dds p\n" +
                        "ON \n" +
                        "    s.product_id = p.id\n" +
                        "GROUP BY \n" +
                        "    p.product_name\n" +
                        "ORDER BY \n" +
                        "    total_sold DESC\n" +
                        "LIMIT 10"
        );
        writeToClickhouse(productSalesTop10, "productSalesTop10");

        Dataset<Row> productCategoryFull = readFromPostgres(spark,
                "SELECT\n" +
                        "    p.product_category,\n" +
                        "    SUM(s.sale_total_price) AS total_revenue\n" +
                        "FROM\n" +
                        "    postgres.data_mart.sale_dds s\n" +
                        "JOIN\n" +
                        "    postgres.data_mart.product_dds p\n" +
                        "ON\n" +
                        "    s.product_id = p.id\n" +
                        "GROUP BY\n" +
                        "    p.product_category\n" +
                        "ORDER BY\n" +
                        "    total_revenue DESC"
        );
        writeToClickhouse(productCategoryFull, "productCategoryFull");
        Dataset<Row> productSalesInCategory = readFromPostgres(spark,
                "SELECT\n" +
                        "    p.product_category,\n" +
                        "    SUM(s.sale_total_price) AS total_revenue\n" +
                        "FROM\n" +
                        "    postgres.data_mart.sale_dds s\n" +
                        "JOIN\n" +
                        "    postgres.data_mart.product_dds p\n" +
                        "ON\n" +
                        "    s.product_id = p.id\n" +
                        "GROUP BY\n" +
                        "    p.product_category\n" +
                        "ORDER BY\n" +
                        "    total_revenue DESC"
        );
        writeToClickhouse(productSalesInCategory, "productSalesInCategory");
    }

    public void customerSalesShowcase(SparkSession spark) {
        Dataset<Row> customerTop10 = readFromPostgres(spark,
                "SELECT\n" +
                        "    c.customer_first_name,\n" +
                        "    c.customer_last_name,\n" +
                        "    SUM(s.sale_total_price) AS total_spent\n" +
                        "FROM\n" +
                        "    postgres.data_mart.sale_dds s\n" +
                        "JOIN\n" +
                        "    postgres.data_mart.customer_dds c\n" +
                        "ON\n" +
                        "    s.customer_id = c.id\n" +
                        "GROUP BY\n" +
                        "    c.id, c.customer_first_name, c.customer_last_name\n" +
                        "ORDER BY\n" +
                        "    total_spent DESC\n" +
                        "LIMIT 10"
        );
        writeToClickhouse(customerTop10, "customerTop10");

        Dataset<Row> customerCountry = readFromPostgres(spark,
                "SELECT\n" +
                        "    customer_country,\n" +
                        "    COUNT(*) AS customer_count\n" +
                        "FROM\n" +
                        "    postgres.data_mart.customer_dds\n" +
                        "GROUP BY\n" +
                        "    customer_country\n" +
                        "ORDER BY\n" +
                        "    customer_count DESC"
        );
        writeToClickhouse(customerCountry, "customerCountry");

        Dataset<Row> customerCheque = readFromPostgres(spark,
                "SELECT\n" +
                        "    c.customer_first_name,\n" +
                        "    c.customer_last_name,\n" +
                        "    AVG(s.sale_total_price) AS avg_order_value\n" +
                        "FROM\n" +
                        "    postgres.data_mart.sale_dds s\n" +
                        "JOIN\n" +
                        "    postgres.data_mart.customer_dds c\n" +
                        "ON\n" +
                        "    s.customer_id = c.id\n" +
                        "GROUP BY\n" +
                        "    c.id, c.customer_first_name, c.customer_last_name\n" +
                        "ORDER BY\n" +
                        "    avg_order_value DESC"
        );
        writeToClickhouse(customerCheque, "customerCheque");
    }

    public void timeBasedSalesShowcase(SparkSession spark) {
        Dataset<Row> monthTimeBased = readFromPostgres(spark,
                "SELECT\n" +
                        "    TO_CHAR(TO_DATE(s.sale_date, 'MM/DD/YYYY'), 'YYYY-MM') AS sale_month,\n" +
                        "    SUM(s.sale_total_price) AS total_revenue\n" +
                        "FROM\n" +
                        "    postgres.data_mart.sale_dds s\n" +
                        "GROUP BY\n" +
                        "    sale_month\n" +
                        "ORDER BY\n" +
                        "    sale_month"
        );
        writeToClickhouse(monthTimeBased, "monthTimeBased");
        
        Dataset<Row> yearTimeBased = readFromPostgres(spark,
                "SELECT\n" +
                        "    TO_CHAR(TO_DATE(s.sale_date, 'MM/DD/YYYY'), 'YYYY') AS sale_year,\n" +
                        "    SUM(s.sale_total_price) AS total_revenue\n" +
                        "FROM\n" +
                        "    postgres.data_mart.sale_dds s\n" +
                        "GROUP BY\n" +
                        "    sale_year\n" +
                        "ORDER BY\n" +
                        "    sale_year"
        );
        writeToClickhouse(yearTimeBased, "yearTimeBased");
        
        Dataset<Row> revenueComparisonForDifferentPeriods = readFromPostgres(spark,
                "SELECT\n" +
                        "    EXTRACT(YEAR FROM TO_DATE(s.sale_date, 'MM/DD/YYYY')) AS sale_year,\n" +
                        "    SUM(s.sale_total_price) AS total_revenue\n" +
                        "FROM\n" +
                        "    postgres.data_mart.sale_dds s\n" +
                        "GROUP BY\n" +
                        "    sale_year\n" +
                        "ORDER BY\n" +
                        "    sale_year"
        );
        writeToClickhouse(revenueComparisonForDifferentPeriods, "revenueComparisonForDifferentPeriods");
        
        Dataset<Row> averageOrderSizeByMonth = readFromPostgres(spark,
                "SELECT\n" +
                        "    TO_CHAR(TO_DATE(s.sale_date, 'MM/DD/YYYY'), 'YYYY-MM') AS sale_month,\n" +
                        "    AVG(s.sale_total_price) AS avg_order_value\n" +
                        "FROM\n" +
                        "    postgres.data_mart.sale_dds s\n" +
                        "GROUP BY\n" +
                        "    sale_month\n" +
                        "ORDER BY\n" +
                        "    sale_month"
        );
        writeToClickhouse(averageOrderSizeByMonth, "averageOrderSizeByMonth");
    }


    public void storeSalesShowcase(SparkSession spark) {
        Dataset<Row> top5StoresRevenue = readFromPostgres(spark,
                "SELECT\n" +
                        "    st.store_name,\n" +
                        "    SUM(sa.sale_total_price) AS total_revenue\n" +
                        "FROM\n" +
                        "    postgres.data_mart.sale_dds sa\n" +
                        "JOIN postgres.data_mart.product_dds p ON sa.product_id = p.id\n" +
                        "JOIN postgres.data_mart.store_dds st ON p.store_id = st.id\n" +
                        "GROUP BY st.store_name\n" +
                        "ORDER BY total_revenue DESC\n" +
                        "LIMIT 5"
        );
        writeToClickhouse(top5StoresRevenue, "top5StoresRevenue");

        Dataset<Row> storeRevenueByLocation = readFromPostgres(spark,
                "SELECT\n" +
                        "    st.store_city,\n" +
                        "    st.store_country,\n" +
                        "    SUM(sa.sale_total_price) AS total_revenue\n" +
                        "FROM\n" +
                        "    postgres.data_mart.sale_dds sa\n" +
                        "JOIN postgres.data_mart.product_dds p ON sa.product_id = p.id\n" +
                        "JOIN postgres.data_mart.store_dds st ON p.store_id = st.id\n" +
                        "GROUP BY st.store_city, st.store_country\n" +
                        "ORDER BY total_revenue DESC"
        );
        writeToClickhouse(storeRevenueByLocation, "storeRevenueByLocation");

        Dataset<Row> avgCheckPerStore = readFromPostgres(spark,
                "SELECT\n" +
                        "    st.store_name,\n" +
                        "    AVG(sa.sale_total_price) AS avg_order_value\n" +
                        "FROM\n" +
                        "    postgres.data_mart.sale_dds sa\n" +
                        "JOIN postgres.data_mart.product_dds p ON sa.product_id = p.id\n" +
                        "JOIN postgres.data_mart.store_dds st ON p.store_id = st.id\n" +
                        "GROUP BY st.store_name\n" +
                        "ORDER BY avg_order_value DESC"
        );
        writeToClickhouse(avgCheckPerStore, "avgCheckPerStore");
    }

    public void supplierSalesShowcase(SparkSession spark) {
        Dataset<Row> top5SuppliersRevenue = readFromPostgres(spark,
                "SELECT\n" +
                        "    sup.supplier_name,\n" +
                        "    SUM(sa.sale_total_price) AS total_revenue\n" +
                        "FROM\n" +
                        "    postgres.data_mart.sale_dds sa\n" +
                        "JOIN postgres.data_mart.product_dds p ON sa.product_id = p.id\n" +
                        "JOIN postgres.data_mart.supplier_dds sup ON p.supplier_id = sup.id\n" +
                        "GROUP BY sup.supplier_name\n" +
                        "ORDER BY total_revenue DESC\n" +
                        "LIMIT 5"
        );
        writeToClickhouse(top5SuppliersRevenue, "top5SuppliersRevenue");

        Dataset<Row> avgProductPricePerSupplier = readFromPostgres(spark,
                "SELECT\n" +
                        "    sup.supplier_name,\n" +
                        "    AVG(p.product_price) AS avg_product_price\n" +
                        "FROM\n" +
                        "    postgres.data_mart.product_dds p\n" +
                        "JOIN postgres.data_mart.supplier_dds sup ON p.supplier_id = sup.id\n" +
                        "GROUP BY sup.supplier_name\n" +
                        "ORDER BY avg_product_price DESC"
        );
        writeToClickhouse(avgProductPricePerSupplier, "avgProductPricePerSupplier");

        Dataset<Row> salesBySupplierCountry = readFromPostgres(spark,
                "SELECT\n" +
                        "    sup.supplier_country,\n" +
                        "    SUM(sa.sale_total_price) AS total_revenue\n" +
                        "FROM\n" +
                        "    postgres.data_mart.sale_dds sa\n" +
                        "JOIN postgres.data_mart.product_dds p ON sa.product_id = p.id\n" +
                        "JOIN postgres.data_mart.supplier_dds sup ON p.supplier_id = sup.id\n" +
                        "GROUP BY sup.supplier_country\n" +
                        "ORDER BY total_revenue DESC"
        );
        writeToClickhouse(salesBySupplierCountry, "salesBySupplierCountry");
    }

    public void productRatingsShowcase(SparkSession spark) {
        Dataset<Row> topRatedProducts = readFromPostgres(spark,
                "SELECT product_name, product_rating\n" +
                        "FROM postgres.data_mart.product_dds\n" +
                        "ORDER BY product_rating DESC\n" +
                        "LIMIT 5"
        );
        writeToClickhouse(topRatedProducts, "topRatedProducts");

        Dataset<Row> lowestRatedProducts = readFromPostgres(spark,
                "SELECT product_name, product_rating\n" +
                        "FROM postgres.data_mart.product_dds\n" +
                        "ORDER BY product_rating ASC\n" +
                        "LIMIT 5"
        );
        writeToClickhouse(lowestRatedProducts, "lowestRatedProducts");

        Dataset<Row> productsByReviews = readFromPostgres(spark,
                "SELECT product_name, product_reviews\n" +
                        "FROM postgres.data_mart.product_dds\n" +
                        "ORDER BY product_reviews DESC\n" +
                        "LIMIT 10"
        );
        writeToClickhouse(productsByReviews, "productsByReviews");

        Dataset<Row> ratingSalesCorrelation = readFromPostgres(spark,
                "SELECT CORR(p.product_rating, sa.sale_quantity) AS rating_sales_correlation\n" +
                        "FROM postgres.data_mart.sale_dds sa\n" +
                        "JOIN postgres.data_mart.product_dds p ON sa.product_id = p.id"
        );
        writeToClickhouse(ratingSalesCorrelation, "ratingSalesCorrelation");
    }

}
