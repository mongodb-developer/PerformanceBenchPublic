package com.mongodb.jlp.orderbench;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.AggregateIterable;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;

/* This class generates a set of existing data as efficiently as possible */
/* We aregoing to generate only one version of it and then use server side aggregation to */
/* generate the other versions */

public class BulkLoad {
    Logger logger;
    MongoClient mongoClient;
    RecordFactory recordFactory;
    TestOptions testOptions;
    private static Random random = new Random();
    private static int BATCHSIZE = 1000;
    private static int NUM_WAREHOUSES = 20;
    int[] custOrders;
    // We have a fixed number of customers but each could have any number of orders
    // and we need
    // To know the number of items for each
    Map<String, Integer> custOrderItems;

    BulkLoad(MongoClient m, RecordFactory f) {
        logger = LoggerFactory.getLogger(BulkLoad.class);
        mongoClient = m;
        recordFactory = f;

    }

    void loadInitialData(TestOptions options) {
        logger.info("Loading Initial Data Single Collection Style");
        // Keep track of how many orders for each customer to allow serial numbering
        custOrders = new int[options.getInteger("customers")];
        custOrderItems = new HashMap<String, Integer>();

        logger.info(options.toJson());
        testOptions = options;

        dropExistingData();
        loadWarehouses();
        loadProducts();
        loadCustomers();
        loadOrders();
        loadOrderItems();
        loadInvoices();
        loadShipments();

    }

    void dropExistingData() {
        logger.info("Dropping Existing Data");
        mongoClient.getDatabase("orderbench").drop();
    }

    // Small, fixed number of warehouses

    void loadWarehouses() {
        List<Document> warehouses = new ArrayList<Document>();
        logger.info("Loading Warehouse Data");
        for (int c = 0; c < NUM_WAREHOUSES; c++) {
            Document warehouse = recordFactory.getWarehouse(c);

            warehouse.put("_id", warehouse.getString("warehouseId"));
            warehouses.add(warehouse);
        }
        mongoClient.getDatabase("orderbench").getCollection("data").insertMany(warehouses);
    }

    // Configurable - large number of products in warehouses
    void loadProducts() {
        List<Document> products = new ArrayList<Document>();
        logger.info("Loading Product Data");
        for (int c = 0; c < testOptions.getInteger("products"); c++) {
            int whid = random.nextInt(NUM_WAREHOUSES);
            Document product = recordFactory.getProduct(c, whid);
            product.put("_id", product.getString("productId"));
            products.add(product);
            if (c % BATCHSIZE == 0 && products.size() > 0) {
                logger.info("Products: " + c);
                mongoClient.getDatabase("orderbench").getCollection("data").insertMany(products);
                products.clear();
            }
        }
        if (products.size() > 0) {
            mongoClient.getDatabase("orderbench").getCollection("data").insertMany(products);
        }
    }

    void loadCustomers() {
        List<Document> customers = new ArrayList<Document>();
        logger.info("Loading Customer Data");
        for (int c = 0; c < testOptions.getInteger("customers"); c++) {
            Document customer = recordFactory.getCustomer(c);
            customer.put("_id", customer.getString("customerId"));
            customers.add(customer);
            if (c % BATCHSIZE == 0 && customers.size() > 0) {
                logger.info("Customers: " + c);
                mongoClient.getDatabase("orderbench").getCollection("data").insertMany(customers);
                customers.clear();
            }
        }
        if (customers.size() > 0) {
            mongoClient.getDatabase("orderbench").getCollection("data").insertMany(customers);
        }
    }

    void loadOrders() {
        List<Document> orders = new ArrayList<Document>();
        logger.info("Loading Order Data");
        // Orders is average orders per customer
        for (int c = 0; c < testOptions.getInteger("orders") * testOptions.getInteger("customers"); c++) {
            int custid = random.nextInt(testOptions.getInteger("customers"));
            int orderid = ++custOrders[custid];
            Document order = recordFactory.getOrder(orderid, custid);
            String orderPK = order.getString("customerId") + order.getString("orderId");
            order.put("_id", orderPK);
            custOrderItems.put(custid + "_" + orderid, 0);
            orders.add(order);
            if (c % BATCHSIZE == 0 && orders.size() > 0) {
                logger.info("Orders: " + c);
                mongoClient.getDatabase("orderbench").getCollection("data").insertMany(orders);
                orders.clear();
            }
        }
        if (orders.size() > 0) {
            mongoClient.getDatabase("orderbench").getCollection("data").insertMany(orders);
        }
    }

    void loadOrderItems() {
        List<Document> orderItems = new ArrayList<Document>();
        logger.info("Loading orderItems Data");
        // Orders is average orders per customer
        for (int c = 0; c < testOptions.getInteger("orders") * testOptions.getInteger("customers")
                * testOptions.getInteger("items"); c++) {
            int custid = random.nextInt(testOptions.getInteger("customers"));
            int orderid = random.nextInt(custOrders[custid]) + 1;
            // Shoudl encapsulate this logic really

            int orderitemId = custOrderItems.get(custid + "_" + orderid) + 1;
            custOrderItems.put(custid + "_" + orderid, orderitemId);
            int productid = random.nextInt(testOptions.getInteger("products"));
            Document orderItem = recordFactory.getOrderItem(custid, orderid, orderitemId, productid,
                    testOptions.getInteger("size"));
            String orderItemPK = orderItem.getString("customerId") + orderItem.getString("orderId")
                    + orderItem.getString("itemId");
            orderItem.put("_id", orderItemPK);
            orderItems.add(orderItem);

            if (c % BATCHSIZE == 0 && orderItems.size() > 0) {
                logger.info("OrderItems: " + c);
                mongoClient.getDatabase("orderbench").getCollection("data").insertMany(orderItems);
                orderItems.clear();
            }
        }
        if (orderItems.size() > 0) {
            mongoClient.getDatabase("orderbench").getCollection("data").insertMany(orderItems);
        }
    }

    /* Every order has an invoice but we don't want to load them ordered */
    /* custOrderItems is a hash so we can iterate that */
    void loadInvoices() {
        List<Document> invoices = new ArrayList<Document>();
        int c = 0;
        logger.info("Loading INVOICE Data");
        for (String key : custOrderItems.keySet()) {
            String[] parts = key.split("_");
            int custid = Integer.parseInt(parts[0]);
            int orderid = Integer.parseInt(parts[1]);
            Document invoice = recordFactory.getInvoice(custid, orderid, 1);// One per order I guess
            String invoicePK = invoice.getString("customerId") + invoice.getString("orderId")
                    + invoice.getString("invoiceId");
            invoice.put("_id", invoicePK);
            invoices.add(invoice);
            c++;
            if (c % BATCHSIZE == 0 && invoices.size() > 0) {
                logger.info("Invoices: " + c);
                mongoClient.getDatabase("orderbench").getCollection("data").insertMany(invoices);
                invoices.clear();
            }
        }
        if (invoices.size() > 0) {
            mongoClient.getDatabase("orderbench").getCollection("data").insertMany(invoices);
        }
    }

    // Loading shipments and the items in them (Join table) together here as that
    // seems OK
    void loadShipments() {
        List<Document> shipments = new ArrayList<Document>();
        List<Document> shipmentItems = new ArrayList<Document>();
        int c = 0;
        logger.info("Loading shipment Data");
        for (String key : custOrderItems.keySet()) {
            String[] parts = key.split("_");
            int custid = Integer.parseInt(parts[0]);
            int orderid = Integer.parseInt(parts[1]);
            int nItems = custOrderItems.get(key);
            int fromItem = 0;
            int shipmentid = 1;
            int itemsshipped = 0;
            do {

                int warehouseid = random.nextInt(NUM_WAREHOUSES);
                int itemsinshipment = random.nextInt(5) + 1;
                Document shipment = recordFactory.getShipment(custid, orderid, shipmentid, warehouseid);
                itemsshipped += itemsinshipment;
                String shipmentPK = shipment.getString("customerId") + shipment.getString("orderId")
                        + shipment.getString("shipmentId");
                shipment.put("_id", shipmentPK);

                shipments.add(shipment);

                for (int si = fromItem; si < fromItem + itemsinshipment; si++) {
                    Document shippedItem = recordFactory.getShipItem(custid, orderid, shipmentid, si);
                    String shippedItemPK = shippedItem.getString("customerId") + shippedItem.getString("orderId")
                            + shippedItem.getString("shipmentId") + shippedItem.getString("shipmentItemId");
                    shippedItem.put("_id", shippedItemPK);
                    shipmentItems.add(shippedItem);
                }
                fromItem += itemsinshipment;
                shipmentid++;
            } while (itemsshipped < nItems);
            c++;
            if (c % BATCHSIZE == 0 && shipments.size() > 0) {
                logger.info("shipments: " + c);
                mongoClient.getDatabase("orderbench").getCollection("data").insertMany(shipments);
                mongoClient.getDatabase("orderbench").getCollection("data").insertMany(shipmentItems);
                shipments.clear();
                shipmentItems.clear();
            }
        }
        if (shipments.size() > 0) {
            mongoClient.getDatabase("orderbench").getCollection("data").insertMany(shipments);
            mongoClient.getDatabase("orderbench").getCollection("data").insertMany(shipmentItems);
        }
    }

}
