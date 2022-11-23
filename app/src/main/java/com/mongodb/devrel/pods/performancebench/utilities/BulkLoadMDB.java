/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mongodb.devrel.pods.performancebench.utilities;

/**
 *
 * @author graeme
 */
import java.io.IOException;
import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.mongodb.client.MongoClient;
import org.json.simple.JSONObject;

 /* This class generates a set of existing data as efficiently as possible */
 /* We are going to generate only one version of it and then use server side aggregation to */
 /* generate the other versions */
public class BulkLoadMDB {

    Logger logger;
    MongoClient mongoClient;
    RecordFactory recordFactory;
    JSONObject testOptions;
    private static final Random random = new Random();
    private static final int BATCHSIZE = 1000;
    private static final int NUM_WAREHOUSES = 20;
    int[] custOrders;
    // we have a fixed number of customers but each could have any number of orders
    // and we need
    // To know the number of items for each
    Map<String, Integer> custOrderItems;

    public BulkLoadMDB(MongoClient m, RecordFactory f) {
        logger = LoggerFactory.getLogger(BulkLoadMDB.class);
        mongoClient = m;
        recordFactory = f;

    }

    public void loadInitialData(JSONObject options) {
        logger.info("Loading Initial Data");
        // Keep track of how many orders for each customer to allow serial numbering
        custOrders = new int[((Long)options.get("customers")).intValue() + 1];// Index from 1
        custOrderItems = new HashMap<>();

        logger.debug(options.toJSONString());
        testOptions = options;

        dropExistingData();
        //Check for a dump file matching the current test run parameters. If it exists, load the data using it.
        String dumpFile = "singletable_data-c" + options.get("customers").toString();
        dumpFile += "-o" + options.get("orders").toString();
        dumpFile += "-i" + options.get("items").toString();
        dumpFile += "-p" + options.get("products").toString();
        dumpFile += "-s" + options.get("size").toString();
        dumpFile += ".json";
        File f = new File(dumpFile);
        Boolean dataLoaded = false;
        String cwd = Paths.get(".").toAbsolutePath().normalize().toString();
        if(f.exists() && !f.isDirectory()) {
            //Build command
            List<String> commands = new ArrayList<String>();
            commands.add("mongoimport");
            commands.add("--uri");
            commands.add(options.get("uri").toString());
            commands.add("--collection");
            commands.add(options.get("collectionName").toString());
            commands.add("--type");
            commands.add("JSON");
            commands.add("--file");
            commands.add(dumpFile);
            System.out.println(commands);

            ProcessBuilder pb = new ProcessBuilder(commands);
            //pb.directory(new File("/home/narek"));
            pb.redirectErrorStream(true);
            try {
                Process process = pb.start();
                if (process.waitFor() == 0) {
                    dataLoaded = true;
                }
            } catch (IOException e) {
                dataLoaded = false;
            } catch (InterruptedException e) {
                dataLoaded = false;
            }
        }
        if(!dataLoaded){
            //If either a prior dumpfile can't be located or there was a problem restoring it, go ahead and
            //rebuild the data set.
            dropExistingData();
            loadWarehouses();
            loadProducts();
            loadCustomers();
            loadOrders();
            loadOrderItems();
            loadInvoices();
            loadShipments();
            //Dump the database for future use.
            List<String> commands = new ArrayList<String>();
            commands.add("mongoexport");
            commands.add("--uri");
            commands.add(options.get("uri").toString());
            commands.add("--collection");
            commands.add(options.get("collectionName").toString());
            commands.add("--type");
            commands.add("JSON");
            commands.add("--out");
            commands.add(dumpFile);
            System.out.println(commands);

            ProcessBuilder pb = new ProcessBuilder(commands);
            pb.redirectErrorStream(true);
            try {
                Process process = pb.start();
            } catch (IOException e) {
                //Ignore it - if there was a problem, we'll just do a complete data rebuild on the next run.
            }
        }
    }

    void dropExistingData() {
        logger.debug("Dropping Existing Data");
        mongoClient.getDatabase((String)testOptions.get("dbname")).drop();
    }

    // Small, fixed number of warehouses
    void loadWarehouses() {
        List<Document> warehouses = new ArrayList<>();
        logger.debug("Loading Warehouse Data");
        for (int c = 1; c <= NUM_WAREHOUSES; c++) {
            Document warehouse = recordFactory.getWarehouse(c);

            warehouse.put("_id", String.format("W#%d", warehouse.getInteger("warehouseId")));
            warehouses.add(warehouse);
        }
        mongoClient.getDatabase((String)testOptions.get("dbname")).getCollection((String)testOptions.get("collectionName")).insertMany(warehouses);
    }

    // Configurable - large number of products in warehouses
    void loadProducts() {
        List<Document> products = new ArrayList<>();
        logger.debug("Loading Product Data");
        for (int c = 1; c <= ((Long)testOptions.get("products")).intValue(); c++) {
            int whid = random.nextInt(NUM_WAREHOUSES);
            Document product = recordFactory.getProduct(c, whid);
            product.put("_id", String.format("P#%d", product.getInteger("productId")));
            products.add(product);
            if (c % BATCHSIZE == 0 && !products.isEmpty()) {
                logger.debug("Products: " + c);
                mongoClient.getDatabase((String)testOptions.get("dbname")).getCollection((String)testOptions.get("collectionName")).insertMany(products);
                products.clear();
            }
        }
        if (!products.isEmpty()) {
            mongoClient.getDatabase((String)testOptions.get("dbname")).getCollection((String)testOptions.get("collectionName")).insertMany(products);
        }
    }

    void loadCustomers() {
        List<Document> customers = new ArrayList<>();
        logger.debug("Loading Customer Data");
        for (int c = 1; c <= ((Long)testOptions.get("customers")).intValue(); c++) {
            Document customer = recordFactory.getCustomer(c);
            customer.put("_id", String.format("C#%d", customer.getInteger("customerId")));
            customers.add(customer);
            if (c % BATCHSIZE == 0 && !customers.isEmpty()) {
                logger.debug("Customers: " + c);
                mongoClient.getDatabase((String)testOptions.get("dbname")).getCollection((String)testOptions.get("collectionName")).insertMany(customers);
                customers.clear();
            }
        }
        if (!customers.isEmpty()) {
            mongoClient.getDatabase((String)testOptions.get("dbname")).getCollection((String)testOptions.get("collectionName")).insertMany(customers);
        }
    }

    void loadOrders() {
        List<Document> orders = new ArrayList<>();
        logger.debug("Loading Order Data");
        // Orders is average orders per customer
        for (int c = 0; c < ((Long)testOptions.get("orders")).intValue() * ((Long)testOptions.get("customers")).intValue(); c++) {
            int custid = random.nextInt(((Long)testOptions.get("customers")).intValue()) + 1;
            int orderid = ++custOrders[custid];
            Document order = recordFactory.getOrder(orderid, custid);
            String orderPK = String.format("C#%d#O#%d", order.getInteger("customerId"), order.getInteger("orderId"));
            order.put("_id", orderPK);
            custOrderItems.put(custid + "_" + orderid, 0);
            orders.add(order);
            if (c % BATCHSIZE == 0 && !orders.isEmpty()) {
                logger.debug("Orders: " + c);
                mongoClient.getDatabase((String)testOptions.get("dbname")).getCollection((String)testOptions.get("collectionName")).insertMany(orders);
                orders.clear();
            }
        }
        if (!orders.isEmpty()) {
            mongoClient.getDatabase((String)testOptions.get("dbname")).getCollection((String)testOptions.get("collectionName")).insertMany(orders);
        }
    }

    void loadOrderItems() {
        List<Document> orderItems = new ArrayList<>();
        logger.debug("Loading orderItems Data");
        // Orders is average orders per customer
        for (int c = 0; c < ((Long)testOptions.get("orders")).intValue() * ((Long)testOptions.get("customers")).intValue()
                * ((Long)testOptions.get("items")).intValue(); c++) {
            int custid = random.nextInt(((Long)testOptions.get("customers")).intValue()) + 1;
            int orderid = random.nextInt(custOrders[custid]) + 1;
            // Shoudl encapsulate this logic really

            int orderitemId = custOrderItems.get(custid + "_" + orderid) + 1;
            custOrderItems.put(custid + "_" + orderid, orderitemId);
            int productid = random.nextInt(((Long)testOptions.get("products")).intValue()) + 1;
            Document orderItem = recordFactory.getOrderItem(custid, orderid, orderitemId, productid,
                    ((Long)testOptions.get("size")).intValue());
            String orderItemPK = String.format("C#%d#O#%d#I#%d", orderItem.getInteger("customerId"),
                    orderItem.getInteger("orderId"), orderItem.getInteger("itemId"));
            orderItem.put("_id", orderItemPK);
            orderItems.add(orderItem);

            if (c % BATCHSIZE == 0 && !orderItems.isEmpty()) {
                logger.debug("OrderItems: " + c);
                mongoClient.getDatabase((String)testOptions.get("dbname")).getCollection((String)testOptions.get("collectionName")).insertMany(orderItems);
                orderItems.clear();
            }
        }
        if (!orderItems.isEmpty()) {
            mongoClient.getDatabase((String)testOptions.get("dbname")).getCollection((String)testOptions.get("collectionName")).insertMany(orderItems);
        }
    }

    /* Every order has an invoice but we don't want to load them ordered */
 /* custOrderItems is a hash so we can iterate that */
    void loadInvoices() {
        List<Document> invoices = new ArrayList<>();
        int c = 0;
        logger.debug("Loading INVOICE Data");
        for (String key : custOrderItems.keySet()) {
            String[] parts = key.split("_");
            int custid = Integer.parseInt(parts[0]);
            int orderid = Integer.parseInt(parts[1]);
            Document invoice = recordFactory.getInvoice(custid, orderid, 1);// One per order I guess
            String invoicePK = String.format("C#%d#O#%d#IN#%d", invoice.getInteger("customerId"),
                    invoice.getInteger("orderId"), invoice.getInteger("invoiceId"));
            invoice.put("_id", invoicePK);
            invoices.add(invoice);
            c++;
            if (c % BATCHSIZE == 0 && !invoices.isEmpty()) {
                logger.debug("Invoices: " + c);
                mongoClient.getDatabase((String)testOptions.get("dbname")).getCollection((String)testOptions.get("collectionName")).insertMany(invoices);
                invoices.clear();
            }
        }
        if (!invoices.isEmpty()) {
            mongoClient.getDatabase((String)testOptions.get("dbname")).getCollection((String)testOptions.get("collectionName")).insertMany(invoices);
        }
    }

    // Loading shipments and the items in them (Join table) together here as that
    // seems OK
    void loadShipments() {
        List<Document> shipments = new ArrayList<>();
        List<Document> shipmentItems = new ArrayList<>();
        int c = 0;
        logger.debug("Loading shipment Data");
        for (String key : custOrderItems.keySet()) {
            String[] parts = key.split("_");
            int custid = Integer.parseInt(parts[0]);
            int orderid = Integer.parseInt(parts[1]);
            int nItems = custOrderItems.get(key);
            int fromItem = 0;
            int shipmentid = 1;
            int itemsshipped = 0;
            do {

                int warehouseid = random.nextInt(NUM_WAREHOUSES) + 1;
                int itemsinshipment = random.nextInt(5) + 1;
                Document shipment = recordFactory.getShipment(custid, orderid, shipmentid, warehouseid);
                itemsshipped += itemsinshipment;
                String shipmentPK = String.format("C#%d#O#%d#S#%d", shipment.getInteger("customerId"),
                        shipment.getInteger("orderId"), shipment.getInteger("shipmentId"));
                shipment.put("_id", shipmentPK);

                shipments.add(shipment);

                for (int si = fromItem; si < fromItem + itemsinshipment; si++) {
                    Document shippedItem = recordFactory.getShipItem(custid, orderid, shipmentid, si);
                    String shippedItemPK = String.format("C#%d#O#%d#S#%d#I%d", shippedItem.getInteger("customerId"),
                            shippedItem.getInteger("orderId"),
                            shippedItem.getInteger("shipmentId"),
                            shippedItem.getInteger("shipmentItemId"));
                    shippedItem.put("_id", shippedItemPK);
                    shipmentItems.add(shippedItem);
                }
                fromItem += itemsinshipment;
                shipmentid++;
            } while (itemsshipped < nItems);
            c++;
            if (c % BATCHSIZE == 0 && !shipments.isEmpty()) {
                logger.debug("shipments: " + c);
                mongoClient.getDatabase((String)testOptions.get("dbname")).getCollection((String)testOptions.get("collectionName")).insertMany(shipments);
                mongoClient.getDatabase((String)testOptions.get("dbname")).getCollection((String)testOptions.get("collectionName")).insertMany(shipmentItems);
                shipments.clear();
                shipmentItems.clear();
            }
        }
        if (!shipments.isEmpty()) {
            mongoClient.getDatabase((String)testOptions.get("dbname")).getCollection((String)testOptions.get("collectionName")).insertMany(shipments);
            mongoClient.getDatabase((String)testOptions.get("dbname")).getCollection((String)testOptions.get("collectionName")).insertMany(shipmentItems);
        }
    }

}
