package org.example;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class CustomerDdsSink extends RichSinkFunction<MockDataEvent> {

    private Connection conn;
    private PreparedStatement stmt;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.postgresql.Driver");
        conn = DriverManager.getConnection("jdbc:postgresql://postgres:5432/postgres",
                "postgres",
                "postgres");

        stmt = conn.prepareStatement(
            "INSERT INTO postgres.dds.customer_ods " +
            "(customer_first_name, customer_last_name, " +
                    "customer_age, customer_email," +
                    " customer_country, customer_postal_code," +
                    " customer_pet_type, customer_pet_name," +
                    " customer_pet_breed) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
        );
    }

    @Override
    public void invoke(MockDataEvent e, Context ctx) throws Exception {
        stmt.setString(1, e.getCustomerFirstName());
        stmt.setString(2, e.getCustomerLastName());
        stmt.setObject(3, safeInt(e.getCustomerAge()));
        stmt.setString(4, e.getCustomerEmail());
        stmt.setString(5, e.getCustomerCountry());
        stmt.setString(6, e.getCustomerPostalCode());
        stmt.setString(7, e.getCustomerPetType());
        stmt.setString(8, e.getCustomerPetName());
        stmt.setString(9, e.getCustomerPetBreed());

        stmt.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        if (stmt != null) stmt.close();
        if (conn != null) conn.close();
    }

    private Integer safeInt(String val) {
        try { return val != null ? Integer.parseInt(val) : null; }
        catch (NumberFormatException ex) { return null; }
    }
}

