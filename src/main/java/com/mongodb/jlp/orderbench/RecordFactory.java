package com.mongodb.jlp.orderbench;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.charset.Charset;
import java.util.Random;
import java.util.Calendar;

/* This class creates instances of all the record/document types we want to use*/

public class RecordFactory {
    Logger logger;
    private static Random random = new Random();
    private static Calendar cal = Calendar.getInstance();

    RecordFactory() {
        logger = LoggerFactory.getLogger(RecordFactory.class);
    }

    private static String getString(int length) {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder string = new StringBuilder();

        while (string.length() < length)
            string.append(chars.charAt(random.nextInt(chars.length())));

        return string.toString();
    }

    Document getWarehouse(int whid) {
        Document whDoc;
        if (whid % 2 == 0) {
            whDoc = Document.parse(
                    "{\"Country\":\"Sweden\",\"County\":\"Vastra Gotaland\",\"City\":\"Goteborg\",\"Street\":\"MainStreet\",\"Number\":20,\"ZipCode\":41111}");
        } else {
            whDoc = Document.parse(
                    "{\"Country\":\"Sweden\",\"County\":\"Vastra Gotaland\",\"City\":\"Boras\",\"Street\":\"RiverStreet\",\"Number\":20,\"ZipCode\":11111}");
        }
        whDoc.put("warehouseId", String.format("W#%d", whid));
        return whDoc;
    }

    Document getCustomer(int custid) {
        Document custDoc = new Document();
        String email = String.format("%s@somewhere.com", getString(10));
        custDoc.put("customerId", String.format("C#%d", custid));
        custDoc.put("email", email);
        custDoc.put("type", "customer");
        custDoc.put("data", new String(new byte[random.nextInt(6400)], Charset.forName("UTF-8")));
        return custDoc;
    }

    Document getProduct(int productid, int warehouseid) {
        Document prodDoc = new Document();
        prodDoc.put("productId", String.format("P#%d", productid)); // TODO - pull out this formatting to a new class
        prodDoc.put("warehouseId", warehouseid);
        prodDoc.put("type", "product");
        prodDoc.put("name", "Product_" + productid);
        prodDoc.put("qty", random.nextInt(100) + 100);
        prodDoc.put("price", random.nextInt(50) + 10);
        prodDoc.put("description", new String(new byte[random.nextInt(50)], Charset.forName("UTF-8")));
        return prodDoc;
    }

    Document getOrder(int orderid, int custid) {
        Document orderDoc = new Document();
        orderDoc.put("orderId", String.format("O#%d", orderid));
        orderDoc.put("customerId", String.format("C#%d", custid));
        orderDoc.put("ammount", 0); // will be updated later
        orderDoc.put("type", "order");
        cal.add(Calendar.DAY_OF_YEAR, -random.nextInt(30));
        orderDoc.put("date", cal.getTime());
        orderDoc.put("description", new String(new byte[random.nextInt(50)], Charset.forName("UTF-8")));
        return orderDoc;
    }

    Document getOrderItem(int custid, int orderid, int itemid, int productid, int datasize) {
        Document orderItemDoc = new Document();
        orderItemDoc.put("type", "orderitem");
        orderItemDoc.put("productId", String.format("P#%d", productid));
        orderItemDoc.put("date", cal.getTime());
        orderItemDoc.put("orderId", String.format("O#%d", orderid));
        orderItemDoc.put("customerId", String.format("C#%d", custid));
        orderItemDoc.put("itemId", String.format("I#%d", itemid));

        orderItemDoc.put("qty", random.nextInt(10) + 1);
        orderItemDoc.put("price", random.nextInt(50) + 10);
        orderItemDoc.put("details", new String(new byte[random.nextInt(50)], Charset.forName("UTF-8")));
        orderItemDoc.put("data", new String(new byte[random.nextInt(datasize)], Charset.forName("UTF-8")));
        return orderItemDoc;
    }

    Document getInvoice(int custid, int orderid, int invoiceid) {
        Document invoiceDoc = new Document();
        cal.add(Calendar.DAY_OF_YEAR, 1);
        invoiceDoc.put("type", "invoice");
        invoiceDoc.put("invoiceId", String.format("IN#%d", invoiceid));
        invoiceDoc.put("date", cal.getTime());
        invoiceDoc.put("customerId", String.format("C#%d", custid));
        invoiceDoc.put("ammount", (double) random.nextInt(100000) / 10.0);
        invoiceDoc.put("orderId", String.format("O#%d", orderid));
        return invoiceDoc;
    }

    Document getShipment(int customerid, int orderid, int shipmentid, int warehouseid) {
        Document shipmentDoc = new Document();
        shipmentDoc.put("customerId", String.format("C#%d", customerid));
        shipmentDoc.put("orderId", String.format("O#%d", orderid));
        shipmentDoc.put("shipmentId", String.format("S#%d", shipmentid));
        shipmentDoc.put("type", "shipment");
        shipmentDoc.put("date", cal.getTime());
        shipmentDoc.put("shipTo", new Document().append("Country", "Sweden").append("County", "Vastra Gotaland")
                .append("City", "Goteborg").append("Street", "Slanbarsvagen")
                .append("Number", 34).append("ZipCode", 41787));
        String method = (random.nextBoolean() ? "Express" : "Standard");
        shipmentDoc.put("method", method);
        return shipmentDoc;
    }

    Document getShipItem(int customerid, int orderid, int shipmentid, int shipitemid) {
        Document shipItemDoc = new Document();
        shipItemDoc.put("customerId", String.format("C#%d", customerid));
        shipItemDoc.put("orderId", String.format("O#%d", orderid));
        shipItemDoc.put("shipmentId", String.format("S#%d", shipmentid));
        shipItemDoc.put("shipmentItemId", String.format("SI#%d", shipitemid));
        shipItemDoc.put("type", "shipmentitem");
        return shipItemDoc;
    }
}
