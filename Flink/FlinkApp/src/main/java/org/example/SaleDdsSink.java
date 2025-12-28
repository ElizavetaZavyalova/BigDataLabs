package org.example;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.math.BigDecimal;

public class SaleDdsSink extends RichSinkFunction<MockDataEvent> {

    private Connection conn;
    private PreparedStatement stmt;
    private PreparedStatement customerIdStmt;
    private PreparedStatement productIdStmt;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.postgresql.Driver");
        conn = DriverManager.getConnection("jdbc:postgresql://postgres:5432/postgres",
                "postgres",
                "postgres");


        stmt = conn.prepareStatement(
            "INSERT INTO postgres.dds.sale_ods " +
            "(customer_id, product_id, sale_date, sale_quantity, sale_total_price) " +
            "VALUES (?, ?, ?, ?, ?)"
        );


        customerIdStmt = conn.prepareStatement(
            "SELECT id FROM postgres.dds.customer_ods WHERE customer_email = ? LIMIT 1"
        );


        productIdStmt = conn.prepareStatement(
            "SELECT id FROM postgres.dds.product_ods WHERE product_name = ? LIMIT 1"
        );
    }

    @Override
    public void invoke(MockDataEvent e, Context ctx) throws Exception {

        customerIdStmt.setString(1, e.getCustomerEmail());
        ResultSet rsCustomer = customerIdStmt.executeQuery();
        Integer customerId = null;
        if (rsCustomer.next()) {
            customerId = rsCustomer.getInt("id");
        }
        rsCustomer.close();


        productIdStmt.setString(1, e.getProductName());
        ResultSet rsProduct = productIdStmt.executeQuery();
        Integer productId = null;
        if (rsProduct.next()) {
            productId = rsProduct.getInt("id");
        }
        rsProduct.close();

        if (customerId != null && productId != null) {
            stmt.setInt(1, customerId);
            stmt.setInt(2, productId);
            stmt.setString(3, e.getSaleDate());
            stmt.setObject(4, safeInt(e.getSaleQuantity()));
            stmt.setObject(5, safeBigDecimal(e.getSaleTotalPrice()));
            stmt.executeUpdate();
        } else {

            System.out.println("Пропущена запись: customer=" + e.getCustomerEmail() + ", product=" + e.getProductName());
        }
    }

    @Override
    public void close() throws Exception {
        if (stmt != null) stmt.close();
        if (customerIdStmt != null) customerIdStmt.close();
        if (productIdStmt != null) productIdStmt.close();
        if (conn != null) conn.close();
    }

    private Integer safeInt(String val) {
        try { return val != null ? Integer.parseInt(val) : null; }
        catch (NumberFormatException ex) { return null; }
    }

    private BigDecimal safeBigDecimal(String val) {
        try { return val != null ? new BigDecimal(val) : null; }
        catch (NumberFormatException ex) { return null; }
    }
}