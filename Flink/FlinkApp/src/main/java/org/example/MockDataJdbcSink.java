package org.example;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MockDataJdbcSink extends RichSinkFunction<MockDataEvent> {

    private Connection conn;
    private PreparedStatement stmt;
    private int batchSize = 1000;
    private int batchCount = 0;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.postgresql.Driver");
        conn = DriverManager.getConnection(
                "jdbc:postgresql://postgres:5432/postgres",
                "postgres",
                "postgres"
        );

        stmt = conn.prepareStatement("INSERT INTO postgres.ods.mock_data (" +
                "id," +
                " customer_first_name," +
                " customer_last_name, customer_age, " +
                "customer_email, customer_country, " +
                "customer_postal_code, " +
                "customer_pet_type," +
                " customer_pet_name," +
                " customer_pet_breed," +

                "seller_first_name," +
                " seller_last_name," +
                " seller_email, " +
                "seller_country, " +
                "seller_postal_code," +

                "product_name, " +
                "product_category, p" +
                "roduct_price," +
                " product_quantity," +

                "sale_date, " +
                "sale_customer_id," +
                " sale_seller_id, " +
                "sale_product_id, " +
                "sale_quantity, " +
                "sale_total_price," +

                "store_name, " +
                "store_location," +
                " store_city," +
                " store_state, " +
                "store_country, " +
                "store_phone, " +
                "store_email," +
                " pet_category," +

                "product_weight, " +
                "product_color," +
                " product_size, " +
                "product_brand, " +
                "product_material, " +
                "product_description, " +
                "product_rating," +
                " product_reviews, " +
                "product_release_date, " +
                "product_expiry_date," +

                "supplier_name, " +
                "supplier_contact, " +
                "supplier_email, " +
                "supplier_phone, " +
                "supplier_address, " +
                "supplier_city," +
                " supplier_country" +
                ") VALUES (" +
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?," +
                "?, ?, ?, ?, ?," +
                "?, ?, ?, ?," +
                "?, ?, ?, ?, ?, ?," +
                "?, ?, ?, ?, ?, ?, ?, ?," +
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?," +
                "?, ?, ?, ?, ?, ?, ?" +
                ")"
        );
    }

    @Override
    public void invoke(MockDataEvent e, Context ctx) throws Exception {
        int i = 1;

        // Customer
        stmt.setObject(i++, safeInt(e.getId()));
        stmt.setString(i++, e.getCustomerFirstName());
        stmt.setString(i++, e.getCustomerLastName());
        stmt.setObject(i++, safeInt(e.getCustomerAge()));
        stmt.setString(i++, e.getCustomerEmail());
        stmt.setString(i++, e.getCustomerCountry());
        stmt.setString(i++, e.getCustomerPostalCode());
        stmt.setString(i++, e.getCustomerPetType());
        stmt.setString(i++, e.getCustomerPetName());
        stmt.setString(i++, e.getCustomerPetBreed());

        // Seller
        stmt.setString(i++, e.getSellerFirstName());
        stmt.setString(i++, e.getSellerLastName());
        stmt.setString(i++, e.getSellerEmail());
        stmt.setString(i++, e.getSellerCountry());
        stmt.setString(i++, e.getSellerPostalCode());

        // Product
        stmt.setString(i++, e.getProductName());
        stmt.setString(i++, e.getProductCategory());
        stmt.setObject(i++, safeBigDecimal(e.getProductPrice()));
        stmt.setObject(i++, safeInt(e.getProductQuantity()));

        // Sale
        stmt.setString(i++, e.getSaleDate());
        stmt.setObject(i++, safeInt(e.getSaleCustomerId()));
        stmt.setObject(i++, safeInt(e.getSaleSellerId()));
        stmt.setObject(i++, safeInt(e.getSaleProductId()));
        stmt.setObject(i++, safeInt(e.getSaleQuantity()));
        stmt.setObject(i++, safeBigDecimal(e.getSaleTotalPrice()));

        // Store
        stmt.setString(i++, e.getStoreName());
        stmt.setString(i++, e.getStoreLocation());
        stmt.setString(i++, e.getStoreCity());
        stmt.setString(i++, e.getStoreState());
        stmt.setString(i++, e.getStoreCountry());
        stmt.setString(i++, e.getStorePhone());
        stmt.setString(i++, e.getStoreEmail());
        stmt.setString(i++, e.getPetCategory());

        // Product details
        stmt.setObject(i++, safeBigDecimal(e.getProductWeight()));
        stmt.setString(i++, e.getProductColor());
        stmt.setString(i++, e.getProductSize());
        stmt.setString(i++, e.getProductBrand());
        stmt.setString(i++, e.getProductMaterial());
        stmt.setString(i++, e.getProductDescription());
        stmt.setObject(i++, safeBigDecimal(e.getProductRating()));
        stmt.setObject(i++, safeInt(e.getProductReviews()));
        stmt.setString(i++, e.getProductReleaseDate());
        stmt.setString(i++, e.getProductExpiryDate());

        // Supplier — теперь 6 колонок, как в таблице
        stmt.setString(i++, e.getSupplierName());
        stmt.setString(i++, e.getSupplierPhone());
        stmt.setString(i++, e.getSupplierContact());
        stmt.setString(i++, e.getSupplierEmail());
        stmt.setString(i++, e.getSupplierAddress());
        stmt.setString(i++, e.getSupplierCity());
        stmt.setString(i++, e.getSupplierCountry());

        stmt.addBatch();
        batchCount++;

        if (batchCount >= batchSize) {
            stmt.executeBatch();
            batchCount = 0;
        }
    }

    @Override
    public void close() throws Exception {
        if (stmt != null) {
            if (batchCount > 0) {
                stmt.executeBatch();
            }
            stmt.close();
        }
        if (conn != null) conn.close();
    }

    private Integer safeInt(String val) {
        try {
            return val != null ? Integer.parseInt(val) : null;
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private BigDecimal safeBigDecimal(String val) {
        try {
            return val != null ? new BigDecimal(val) : null;
        } catch (NumberFormatException ex) {
            return null;
        }
    }
}
