/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mongodb.devrel.pods.performancebench.utilities;

/**
 *
 * @author graeme
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.sql.BatchUpdateException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Date;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.ibatis.jdbc.ScriptRunner;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import org.json.simple.JSONObject;

/* This class generates a set of existing data as efficiently as possible */
 /* We aregoing to generate only one version of it and then use server side aggregation to */
 /* generate the other versions */
public class BulkLoadPG {

    Logger logger;
    Connection pgClient;
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

    public BulkLoadPG(Connection p, RecordFactory f) {
        logger = LoggerFactory.getLogger(BulkLoadPG.class);
        pgClient = p;
        recordFactory = f;

    }

    public void loadInitialData(JSONObject options) {
        logger.info("Loading Initial Data (Postgres)");
        // Keep track of how many orders for each customer to allow serial numbering
        custOrders = new int[((Long)options.get("customers")).intValue() + 1];// Index from 1
        custOrderItems = new HashMap<>();

        logger.debug(options.toJSONString());
        testOptions = options;

        dropExistingData((String)options.get("rebuildScript"));
        loadWarehouses();
        loadProducts();
        loadCustomers();
        loadOrders();
        loadOrderItems();
        loadInvoices();
        loadShipments();

    }

    void dropExistingData(String schemaScript) {
        logger.debug("Dropping Existing Data");
        
        try{
            File file = new File(schemaScript);
            Reader reader = new BufferedReader(new FileReader(file));
            ScriptRunner sr = new ScriptRunner(pgClient);
            sr.setAutoCommit(true);
            sr.setStopOnError(true);
            sr.setLogWriter(null);
            sr.runScript(reader);
        } catch (FileNotFoundException e) {
            logger.error("Error dropping existing Postgres data");
            logger.error(e.getMessage());
            System.exit(1);
        }
    }

    // Small, fixed number of warehouses
    void loadWarehouses() {
        
        try{
            logger.debug("Loading Warehouse Data");
            String INSERT_SQL = "INSERT INTO " + (String)testOptions.get("pgSchema") + "." 
                    + (String)testOptions.get("warehousestable") 
                    + " VALUES (?, ?, ?, ?, ?, ?, ?, ?);";

            PreparedStatement preparedStatement = pgClient.prepareStatement(INSERT_SQL);
            pgClient.setAutoCommit(false);
            for (int c = 1; c <= ((Long)testOptions.get("warehouses")).intValue(); c++) {
                Document warehouse = recordFactory.getWarehouse(c);

                warehouse.put("_id", String.format("W#%d", warehouse.getInteger("warehouseId")));

                preparedStatement.setString(1, warehouse.getString("_id"));
                preparedStatement.setString(2, warehouse.getString("Country"));
                preparedStatement.setString(3, warehouse.getString("County"));
                preparedStatement.setString(4, warehouse.getString("City"));
                preparedStatement.setString(5, warehouse.getString("Street"));
                preparedStatement.setInt(6, warehouse.getInteger("Number"));
                preparedStatement.setInt(7, warehouse.getInteger("ZipCode"));
                preparedStatement.setInt(8, warehouse.getInteger("warehouseId"));
                preparedStatement.addBatch();
            }
            
            preparedStatement.executeBatch();
            pgClient.commit();
            pgClient.setAutoCommit(true);
            
        } catch (BatchUpdateException batchUpdateException) {
            logger.error("Error seeding warehouse Postgres data");
            logger.error(batchUpdateException.getMessage());
            System.exit(1);
        } catch (SQLException e) {
            logger.error("Error seeding warehouse Postgres data");
            logger.error(e.getMessage());
            System.exit(1);
        }
    }

    // Configurable - large number of products in warehouses
    void loadProducts() {
        
        try{
            logger.debug("Loading Product Data");
            String INSERT_SQL = "INSERT INTO " + (String)testOptions.get("pgSchema") + "." 
                    + (String)testOptions.get("productstable") 
                    + " VALUES (?, ?, ?, ?, ?, ?, ?);";

            PreparedStatement preparedStatement = pgClient.prepareStatement(INSERT_SQL);
            pgClient.setAutoCommit(false);
        
            for (int c = 1; c <= ((Long)testOptions.get("products")).intValue(); c++) {
                
                int whid = random.nextInt(NUM_WAREHOUSES);
                Document product = recordFactory.getProduct(c, whid);
                product.put("_id", String.format("P#%d", product.getInteger("productId")));

                preparedStatement.setString(1, product.getString("_id"));
                preparedStatement.setString(2, product.getString("type"));
                preparedStatement.setString(3, product.getString("name"));
                preparedStatement.setString(4, product.getString("description"));
                preparedStatement.setInt(5, product.getInteger("qty"));
                preparedStatement.setInt(6, product.getInteger("productId"));
                preparedStatement.setInt(7, product.getInteger("warehouseId"));
                preparedStatement.addBatch();
                
                
                if (c % BATCHSIZE == 0) {
                    logger.info("Products: " + c);
                    preparedStatement.executeBatch();
                }
                
            }
            preparedStatement.executeBatch();
            pgClient.commit();
            pgClient.setAutoCommit(true);
            
        } catch (BatchUpdateException batchUpdateException) {
            logger.error("Error seeding products Postgres data");
            logger.error(batchUpdateException.getMessage());
            System.exit(1);
        } catch (SQLException e) {
            logger.error("Error seeding products Postgres data");
            logger.error(e.getMessage());
            System.exit(1);
        }
    }

    void loadCustomers() {
        
        try{
            logger.debug("Loading Customer Data");
            String INSERT_SQL = "INSERT INTO " + (String)testOptions.get("pgSchema") + "." 
                    + (String)testOptions.get("customerstable") 
                    + " VALUES (?, ?, ?, ?, ?);";

            PreparedStatement preparedStatement = pgClient.prepareStatement(INSERT_SQL);
            pgClient.setAutoCommit(false);
        
        
            for (int c = 1; c <= ((Long)testOptions.get("customers")).intValue(); c++) {
                Document customer = recordFactory.getCustomer(c);
                customer.put("_id", String.format("C#%d", customer.getInteger("customerId")));
                
                preparedStatement.setString(1, customer.getString("_id"));
                preparedStatement.setString(2, customer.getString("email"));
                preparedStatement.setString(3, customer.getString("type"));
                preparedStatement.setString(4, customer.getString("data"));
                preparedStatement.setInt(5, customer.getInteger("customerId"));
                preparedStatement.addBatch();
                
                if (c % BATCHSIZE == 0) {
                    logger.info("Customers: " + c);
                    preparedStatement.executeBatch();
                }
            }
            preparedStatement.executeBatch();
            pgClient.commit();
            pgClient.setAutoCommit(true);
            
        } catch (BatchUpdateException batchUpdateException) {
            logger.error("Error seeding customers Postgres data");
            logger.error(batchUpdateException.getMessage());
            System.exit(1);
        } catch (SQLException e) {
            logger.error("Error seeding customers Postgres data");
            logger.error(e.getMessage());
            System.exit(1);
        }
    }

    void loadOrders() {
        
        try{
            logger.debug("Loading Order Data");
            String INSERT_SQL = "INSERT INTO " + (String)testOptions.get("pgSchema") + "." 
                    + (String)testOptions.get("orderstable") 
                    + " VALUES (?, ?, ?, ?, ?, ?, ?, ?);";

            PreparedStatement preparedStatement = pgClient.prepareStatement(INSERT_SQL);
            pgClient.setAutoCommit(false);
        
            // Orders is average orders per customer
            for (int c = 0; c < ((Long)testOptions.get("orders")).intValue() * ((Long)testOptions.get("customers")).intValue(); c++) {
                int custid = random.nextInt(((Long)testOptions.get("customers")).intValue()) + 1;
                int orderid = ++custOrders[custid];
                Document order = recordFactory.getOrder(orderid, custid);
                String orderPK = String.format("C#%d#O#%d", order.getInteger("customerId"), order.getInteger("orderId"));
                order.put("_id", orderPK);
                custOrderItems.put(custid + "_" + orderid, 0);
                
                preparedStatement.setString(1, order.getString("_id"));
                preparedStatement.setTimestamp(2, new Timestamp(order.getDate("date").getTime()));
                preparedStatement.setString(3, order.getString("type"));
                preparedStatement.setString(4, order.getString("description"));
                preparedStatement.setInt(5, order.getInteger("ammount"));
                preparedStatement.setInt(6, order.getInteger("customerId"));
                preparedStatement.setInt(7, order.getInteger("orderId"));
                preparedStatement.setTimestamp(8, new Timestamp(new Date().getTime()));
                preparedStatement.addBatch();
                
                if (c % BATCHSIZE == 0) {
                    logger.debug("Orders: " + c);
                    preparedStatement.executeBatch();
                }
            }
            preparedStatement.executeBatch();
            pgClient.commit();
            pgClient.setAutoCommit(true);
            
        } catch (BatchUpdateException batchUpdateException) {
            logger.error("Error seeding orders Postgres data");
            logger.error(batchUpdateException.getMessage());
            System.exit(1);
        } catch (SQLException e) {
            logger.error("Error seeding orders Postgres data");
            logger.error(e.getMessage());
            System.exit(1);
        }
    }

    void loadOrderItems() {
        
        try{
            logger.debug("Loading Order Items Data");
            String INSERT_SQL = "INSERT INTO " + (String)testOptions.get("pgSchema") + "." 
                    + (String)testOptions.get("orderitemstable") 
                    + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

            PreparedStatement preparedStatement = pgClient.prepareStatement(INSERT_SQL);
            pgClient.setAutoCommit(false);        

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
                              
                preparedStatement.setString(1, orderItem.getString("_id"));
                preparedStatement.setTimestamp(2, new Timestamp(orderItem.getDate("date").getTime()));
                preparedStatement.setString(3, orderItem.getString("type"));
                preparedStatement.setString(4, orderItem.getString("details"));
                preparedStatement.setString(5, orderItem.getString("data"));
                preparedStatement.setInt(6, orderItem.getInteger("qty"));
                preparedStatement.setInt(7, orderItem.getInteger("customerId"));
                preparedStatement.setInt(8, orderItem.getInteger("itemId"));
                preparedStatement.setInt(9, orderItem.getInteger("orderId"));
                preparedStatement.setInt(10, orderItem.getInteger("productId"));
                preparedStatement.setInt(11, orderItem.getInteger("price"));
                preparedStatement.addBatch();
                
                if (c % BATCHSIZE == 0) {
                    logger.debug("Order Items: " + c);
                    preparedStatement.executeBatch();
                }
            }
            preparedStatement.executeBatch();
            pgClient.commit();
            pgClient.setAutoCommit(true);
            
        } catch (BatchUpdateException batchUpdateException) {
            logger.error("Error seeding order items Postgres data");
            logger.error(batchUpdateException.getMessage());
            System.exit(1);
        } catch (SQLException e) {
            logger.error("Error seeding order items Postgres data");
            logger.error(e.getMessage());
            System.exit(1);
        }
    }

    /* Every order has an invoice but we don't want to load them ordered */
    /* custOrderItems is a hash so we can iterate that */
    void loadInvoices() {
        
        try{
            logger.debug("Loading Invoice Data");
            String INSERT_SQL = "INSERT INTO " + (String)testOptions.get("pgSchema") + "." 
                    + (String)testOptions.get("invoicestable") 
                    + " VALUES (?, ?, ?, ?, ?, ?, ?);";

            PreparedStatement preparedStatement = pgClient.prepareStatement(INSERT_SQL);
            pgClient.setAutoCommit(false);  

            int c = 0;
            for (String key : custOrderItems.keySet()) {
                String[] parts = key.split("_");
                int custid = Integer.parseInt(parts[0]);
                int orderid = Integer.parseInt(parts[1]);
                Document invoice = recordFactory.getInvoice(custid, orderid, 1);// One per order I guess
                String invoicePK = String.format("C#%d#O#%d#IN#%d", invoice.getInteger("customerId"),
                        invoice.getInteger("orderId"), invoice.getInteger("invoiceId"));
                invoice.put("_id", invoicePK);
                c++;
              
                preparedStatement.setString(1, invoice.getString("_id"));
                preparedStatement.setTimestamp(2, new Timestamp(invoice.getDate("date").getTime()));
                preparedStatement.setString(3, invoice.getString("type"));
                preparedStatement.setInt(4, invoice.getInteger("customerId"));
                preparedStatement.setInt(5, invoice.getInteger("orderId"));
                preparedStatement.setInt(6, invoice.getInteger("invoiceId"));
                preparedStatement.setFloat(7, invoice.getDouble("ammount").floatValue());
                preparedStatement.addBatch();  
                
                if (c % BATCHSIZE == 0) {
                    logger.debug("Invoices: " + c);
                    preparedStatement.executeBatch();
                }
            }
            preparedStatement.executeBatch();
            pgClient.commit();
            pgClient.setAutoCommit(true);
            
        } catch (BatchUpdateException batchUpdateException) {
            logger.error("Error seeding invoices Postgres data");
            logger.error(batchUpdateException.getMessage());
            System.exit(1);
        } catch (SQLException e) {
            logger.error("Error seeding invoices Postgres data");
            logger.error(e.getMessage());
            System.exit(1);
        }
    }

    // Loading shipments and the items in them (Join table) together here as that
    // seems OK
    void loadShipments() {
        
        
        try{
            logger.debug("Loading Shipment Data");
            String INSERT_SQL = "INSERT INTO " + (String)testOptions.get("pgSchema") + "." 
                    + (String)testOptions.get("shipmentstable") 
                    + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

            PreparedStatement preparedStatement = pgClient.prepareStatement(INSERT_SQL);
            
            String ITEMS_SQL = "INSERT INTO " + (String)testOptions.get("pgSchema") + "." 
                    + (String)testOptions.get("shipmentitemstable") 
                    + " VALUES (?, ?, ?, ?, ?, ?);";
            
            PreparedStatement itemsPreparedStatement = pgClient.prepareStatement(ITEMS_SQL);
            
            pgClient.setAutoCommit(false);  

            int c = 0;
            for (String key : custOrderItems.keySet()) {
                String[] parts = key.split("_");
                int custid = Integer.parseInt(parts[0]);
                int orderid = Integer.parseInt(parts[1]);
                int nItems = custOrderItems.get(key);
                int fromItem = 0;
                int shipmentid = 1;
                int itemsshipped = 0;
                do {

                    int warehouseid = random.nextInt(((Long)testOptions.get("warehouses")).intValue()) + 1;
                    int itemsinshipment = random.nextInt(((Long)testOptions.get("shipmentitems")).intValue()) + 1;
                    Document shipment = recordFactory.getShipment(custid, orderid, shipmentid, warehouseid);
                    itemsshipped += itemsinshipment;
                    String shipmentPK = String.format("C#%d#O#%d#S#%d", shipment.getInteger("customerId"),
                            shipment.getInteger("orderId"), shipment.getInteger("shipmentId"));
                    shipment.put("_id", shipmentPK);
                    
                    preparedStatement.setString(1, shipment.getString("_id"));
                    preparedStatement.setInt(2, shipment.getInteger("customerId"));
                    preparedStatement.setInt(3, shipment.getInteger("orderId"));
                    preparedStatement.setInt(4, shipment.getInteger("shipmentId"));
                    preparedStatement.setString(5, shipment.getString("type"));
                    preparedStatement.setTimestamp(6, new Timestamp(shipment.getDate("date").getTime()));
                    preparedStatement.setString(7, shipment.getEmbedded(Arrays.asList(new String[]{"shipTo", "Country"}), String.class));
                    preparedStatement.setString(8, shipment.getEmbedded(Arrays.asList(new String[]{"shipTo", "County"}), String.class));
                    preparedStatement.setString(9, shipment.getEmbedded(Arrays.asList(new String[]{"shipTo", "City"}), String.class));
                    preparedStatement.setString(10, shipment.getEmbedded(Arrays.asList(new String[]{"shipTo", "Street"}), String.class));
                    preparedStatement.setInt(11, shipment.getEmbedded(Arrays.asList(new String[]{"shipTo", "Number"}), Integer.class));
                    preparedStatement.setInt(12, shipment.getEmbedded(Arrays.asList(new String[]{"shipTo", "ZipCode"}), Integer.class));
                    preparedStatement.setString(13, shipment.getString("method"));
                    preparedStatement.addBatch();  

                    for (int si = fromItem; si < fromItem + itemsinshipment; si++) {
                        Document shippedItem = recordFactory.getShipItem(custid, orderid, shipmentid, si);
                        String shippedItemPK = String.format("C#%d#O#%d#S#%d#I%d", shippedItem.getInteger("customerId"),
                                shippedItem.getInteger("orderId"),
                                shippedItem.getInteger("shipmentId"),
                                shippedItem.getInteger("shipmentItemId"));
                        shippedItem.put("_id", shippedItemPK);
                        
                        itemsPreparedStatement.setString(1, shippedItem.getString("_id"));
                        itemsPreparedStatement.setInt(2, shippedItem.getInteger("customerId"));
                        itemsPreparedStatement.setInt(3, shippedItem.getInteger("orderId"));
                        itemsPreparedStatement.setInt(4, shippedItem.getInteger("shipmentId"));
                        itemsPreparedStatement.setInt(5, shippedItem.getInteger("shipmentItemId"));
                        itemsPreparedStatement.setString(6, shippedItem.getString("type"));
                        itemsPreparedStatement.addBatch(); 
                        
                    }
                    fromItem += itemsinshipment;
                    shipmentid++;
                } while (itemsshipped < nItems);
                c++;
                if (c % BATCHSIZE == 0) {
                    logger.debug("Shipments: " + c);
                    preparedStatement.executeBatch();
                    itemsPreparedStatement.executeBatch();
                }
            }
            preparedStatement.executeBatch();
            itemsPreparedStatement.executeBatch();
            pgClient.commit();
            pgClient.setAutoCommit(true);
        } catch (BatchUpdateException batchUpdateException) {
            logger.error("Error seeding shipments Postgres data");
            logger.error(batchUpdateException.getMessage());
            System.exit(1);
        } catch (SQLException e) {
            logger.error("Error seeding shipments Postgres data");
            logger.error(e.getMessage());
            System.exit(1);
        }
    }
}
