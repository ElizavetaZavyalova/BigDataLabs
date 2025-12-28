package org.example;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class MockDataEvent {
    private String id;

    @JsonProperty("customer_first_name")
    private String customerFirstName;

    @JsonProperty("customer_last_name")
    private String customerLastName;

    @JsonProperty("customer_age")
    private String customerAge;

    @JsonProperty("customer_email")
    private String customerEmail;

    @JsonProperty("customer_country")
    private String customerCountry;

    @JsonProperty("customer_postal_code")
    private String customerPostalCode;

    @JsonProperty("customer_pet_type")
    private String customerPetType;

    @JsonProperty("customer_pet_name")
    private String customerPetName;

    @JsonProperty("customer_pet_breed")
    private String customerPetBreed;


    @JsonProperty("seller_first_name")
    private String sellerFirstName;

    @JsonProperty("seller_last_name")
    private String sellerLastName;

    @JsonProperty("seller_email")
    private String sellerEmail;

    @JsonProperty("seller_country")
    private String sellerCountry;

    @JsonProperty("seller_postal_code")
    private String sellerPostalCode;


    @JsonProperty("product_name")
    private String productName;

    @JsonProperty("product_category")
    private String productCategory;

    @JsonProperty("product_price")
    private String productPrice;

    @JsonProperty("product_quantity")
    private String productQuantity;


    @JsonProperty("sale_date")
    private String saleDate;

    @JsonProperty("sale_quantity")
    private String saleQuantity;

    @JsonProperty("sale_total_price")
    private String saleTotalPrice;

    @JsonProperty("sale_customer_id")
    private String saleCustomerId;

    @JsonProperty("sale_seller_id")
    private String saleSellerId;

    @JsonProperty("sale_product_id")
    private String saleProductId;


    @JsonProperty("store_name")
    private String storeName;

    @JsonProperty("store_location")
    private String storeLocation;

    @JsonProperty("store_city")
    private String storeCity;

    @JsonProperty("store_state")
    private String storeState;

    @JsonProperty("store_country")
    private String storeCountry;

    @JsonProperty("store_phone")
    private String storePhone;

    @JsonProperty("store_email")
    private String storeEmail;

    @JsonProperty("pet_category")
    private String petCategory;

    @JsonProperty("product_weight")
    private String productWeight;

    @JsonProperty("product_color")
    private String productColor;

    @JsonProperty("product_size")
    private String productSize;

    @JsonProperty("product_brand")
    private String productBrand;

    @JsonProperty("product_material")
    private String productMaterial;

    @JsonProperty("product_description")
    private String productDescription;

    @JsonProperty("product_rating")
    private String productRating;

    @JsonProperty("product_reviews")
    private String productReviews;

    @JsonProperty("product_release_date")
    private String productReleaseDate;

    @JsonProperty("product_expiry_date")
    private String productExpiryDate;

    @JsonProperty("supplier_name")
    private String supplierName;

    @JsonProperty("supplier_contact")
    private String supplierContact;

    @JsonProperty("supplier_email")
    private String supplierEmail;

    @JsonProperty("supplier_phone")
    private String supplierPhone;

    @JsonProperty("supplier_address")
    private String supplierAddress;

    @JsonProperty("supplier_city")
    private String supplierCity;

    @JsonProperty("supplier_country")
    private String supplierCountry;
}
