/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mongodb.devrel.pods.performancebench.utilities;

/**
 *
 * @author graeme
 */

import java.util.Calendar;
import java.util.Random;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* This class creates instances of all the record/document types we want to use*/
public class RecordFactory {

    Logger logger;
    private static Random random = new Random();
    private static Calendar cal = Calendar.getInstance();

    public RecordFactory() {
        logger = LoggerFactory.getLogger(RecordFactory.class);
    }

    private static String getEmail(int length) {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder string = new StringBuilder();

        while (string.length() < length) {
            string.append(chars.charAt(random.nextInt(chars.length())));
        }

        return string.toString();
    }

    private static String getDescription(int length) {
        String chars = "eeeeeeeeeeaaaaaaaarrrrrrriiiiiiioooooootttttttnnnnnnnsssssslllllcccccuuuudddpppmmmhhhggbbyywkv                  ";
        StringBuilder string = new StringBuilder();

        while (string.length() < length) {
            string.append(chars.charAt(random.nextInt(chars.length())));
        }

        return string.toString();
    }

    public Document getWarehouse(int whid) {
        Document whDoc;
        if (whid % 2 == 0) {
            whDoc = Document.parse(
                    "{\"Country\":\"Sweden\",\"County\":\"Vastra Gotaland\",\"City\":\"Goteborg\",\"Street\":\"MainStreet\",\"Number\":20,\"ZipCode\":41111,\"type\":\"warehouses\"}");
        } else {
            whDoc = Document.parse(
                    "{\"Country\":\"Sweden\",\"County\":\"Vastra Gotaland\",\"City\":\"Boras\",\"Street\":\"RiverStreet\",\"Number\":20,\"ZipCode\":11111,\"type\":\"warehouses\"}");
        }
        whDoc.put("warehouseId", whid);
        return whDoc;
    }

    public Document getCustomer(int custid) {
        Document custDoc = new Document();
        String email = String.format("%s@somewhere.com", getEmail(10));
        custDoc.put("customerId", custid);
        custDoc.put("email", email);
        custDoc.put("type", "customers");
        custDoc.put("data", getDescription(random.nextInt(6400)));
        return custDoc;
    }

    public Document getProduct(int productid, int warehouseid) {
        Document prodDoc = new Document();
        prodDoc.put("productId", productid); // TODO - pull out this formatting to a new class
        prodDoc.put("warehouseId", warehouseid);
        prodDoc.put("type", "products");
        prodDoc.put("name", "Product_" + productid);
        prodDoc.put("qty", random.nextInt(100) + 100);
        prodDoc.put("price", random.nextInt(50) + 10);
        prodDoc.put("description", getDescription(random.nextInt(50)));
        return prodDoc;
    }

    public Document getOrder(int orderid, int custid) {
        Document orderDoc = new Document();
        orderDoc.put("orderId", orderid);
        orderDoc.put("customerId", custid);
        orderDoc.put("ammount", 0); // will be updated later
        orderDoc.put("type", "orders");
        cal.add(Calendar.DAY_OF_YEAR, -random.nextInt(30));
        orderDoc.put("date", cal.getTime());
        orderDoc.put("description", getDescription(random.nextInt(50)));
        return orderDoc;
    }

    public Document getOrderItem(int custid, int orderid, int itemid, int productid, int datasize) {
        Document orderItemDoc = new Document();
        orderItemDoc.put("type", "orderitems");
        orderItemDoc.put("productId", productid);
        orderItemDoc.put("date", cal.getTime());
        orderItemDoc.put("orderId", orderid);
        orderItemDoc.put("customerId", custid);
        orderItemDoc.put("itemId", itemid);

        orderItemDoc.put("qty", random.nextInt(10) + 1);
        orderItemDoc.put("price", random.nextInt(50) + 10);
        orderItemDoc.put("details", getDescription(random.nextInt(50)));
        orderItemDoc.put("data", getDescription(random.nextInt(datasize)));
        return orderItemDoc;
    }

    public Document getInvoice(int custid, int orderid, int invoiceid) {
        Document invoiceDoc = new Document();
        cal.add(Calendar.DAY_OF_YEAR, 1);
        invoiceDoc.put("type", "invoices");
        invoiceDoc.put("invoiceId", invoiceid);
        invoiceDoc.put("date", cal.getTime());
        invoiceDoc.put("customerId", custid);
        invoiceDoc.put("ammount", (double) random.nextInt(100000) / 10.0);
        invoiceDoc.put("orderId", orderid);
        return invoiceDoc;
    }

    public Document getShipment(int customerid, int orderid, int shipmentid, int warehouseid) {
        Document shipmentDoc = new Document();
        shipmentDoc.put("customerId", customerid);
        shipmentDoc.put("orderId", orderid);
        shipmentDoc.put("shipmentId", shipmentid);
        shipmentDoc.put("type", "shipments");
        shipmentDoc.put("date", cal.getTime());
        shipmentDoc.put("shipTo", new Document().append("Country", "Sweden").append("County", "Vastra Gotaland")
                .append("City", "Goteborg").append("Street", "Slanbarsvagen")
                .append("Number", 34).append("ZipCode", 41787));
        String method = (random.nextBoolean() ? "Express" : "Standard");
        shipmentDoc.put("method", method);
        return shipmentDoc;
    }

    public Document getShipItem(int customerid, int orderid, int shipmentid, int shipitemid) {
        Document shipItemDoc = new Document();
        shipItemDoc.put("customerId", customerid);
        shipItemDoc.put("orderId", orderid);
        shipItemDoc.put("shipmentId", shipmentid);
        shipItemDoc.put("shipmentItemId", shipitemid);
        shipItemDoc.put("type", "shipmentitems");
        return shipItemDoc;
    }
}
