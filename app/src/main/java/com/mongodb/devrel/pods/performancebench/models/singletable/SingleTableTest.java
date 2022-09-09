/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mongodb.devrel.pods.performancebench.models.singletable;

/**
 *
 * @author graeme
 */
import com.mongodb.devrel.pods.performancebench.utilities.BulkLoadMDB;
import com.mongodb.devrel.pods.performancebench.utilities.RecordFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.devrel.pods.performancebench.SchemaTest;


import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleTableTest implements SchemaTest {

    Logger logger;
    MongoClient mongoClient;
    MongoCollection<Document> singleTable;
    RecordFactory recordFactory = new RecordFactory();
    JSONObject args;
    JSONObject customArgs;
    private static final Random random = new Random();
    

    public SingleTableTest() {
        logger = LoggerFactory.getLogger(SingleTableTest.class);
    }
    
    @Override
    public void initialize(JSONObject newArgs){
        
        this.args = newArgs;
        this.customArgs = (JSONObject)args.get("custom");
                
        mongoClient = MongoClients.create(customArgs.get("uri").toString());
        // Quick check of connection up front
        Document pingResult = mongoClient.getDatabase("system").runCommand(new Document("hello", 1));
        logger.debug(pingResult.toJson());
        
        singleTable = mongoClient.getDatabase((String)customArgs.get("dbname")).getCollection((String)customArgs.get("collectionName"));

        RecordFactory factory = new RecordFactory();

        BulkLoadMDB bulkLoad = new BulkLoadMDB(mongoClient, factory);

        if ((Boolean)customArgs.get("loadData")) {
            bulkLoad.loadInitialData(customArgs);
            prepareTestData();
        }

        /* Quick check things are working */
        logger.debug("Quick self test - just to see we are getting results");
        int customer = 1;
        int order = 1;


        List<Document> d = getOrderById(customer, order);
        logger.debug(name() + " Result size: " + d.size());
        if (d.isEmpty() || d.size() > 500) {
            logger.error("THIS DOESN'T LOOK CORRECT!!!");
            System.exit(1);
        }
        int changes = addNewShipment(1, 1, ((Long)customArgs.get("items")).intValue() + 1, 5, 1);
        logger.debug(name() + " new Shipment changes: " + changes);
        if (changes == 0) {
            logger.error("THIS DOESN'T LOOK CORRECT!!!");
            System.exit(1);
        }

        changes = updateSingleItem(1, 1, 1);
        logger.debug(name() + " updateItem changes: " + changes);
        if (changes == 0) {
            logger.error("THIS DOESN'T LOOK CORRECT!!! - does Customer1 Order 1 have 0 items?");
            System.exit(1);
        }
    }
    
    /* Simulates N devices inserting X Documents */
    

    // Method to take the Bulk loaded data and prepare it for testing
    // For exmaple to reshape oir index it
    private void prepareTestData() {
        // For now we do nothing for the Single table test we just use as bulk loaded

    }

    @Override
    public String name() {
        return "SingleTableTest";
    }

    @Override
    public void warmup() {
        singleTable.find(new Document("not", "true")).first(); // Collection scan will pull it all into cache if it can
        singleTable.find(new Document("not", "true")).first(); // Collection scan will pull it all into cache if it can
        // If it doesnt fit then we will part fill the cache;

    }

    @Override
    public void cleanup() {
        mongoClient.close();
    }
    
    // Order may be in multiple docs
    public List<Document> getOrderById(int customerId, int orderId) {
        List<Document> rval = new ArrayList<>();
        String orderPrefix = String.format("C#%d#O#%d", customerId, orderId);
        // Querying with _id field to avoid requiring index on OrderId - not sure that's
        // a good idea from a readability perspective but Rick H wanted to try that
        // so the query is Orderid <= _id <= Orderid + "$" (as we want orderid or
        // orderid#....)
        // Secondary indexes are fine in MongoDB :-)

        Bson query = Filters.and(Filters.gte("_id", orderPrefix), Filters.lt("_id", orderPrefix + "$"));
        return singleTable.find(query).into(rval);
    }

    // Greates a new shipment for all of the items in this order
    // Don't worry if they were already shipped - imagine they got lost
    // we only want to test the write so lets assume we already have the order doc
    // And select N items to ship where 1<N<NitemsMax
    public int addNewShipment(int custid, int orderid, int shipmentid, int itemsinshipment, int warehouseid) {
        InsertManyResult rval;

        Document shipment = recordFactory.getShipment(custid, orderid, shipmentid, warehouseid);

        String shipmentPK = String.format("C#%d#O%d#S#%d", shipment.getInteger("customerId"),
                shipment.getInteger("orderId"), shipment.getInteger("shipmentId"));

        shipment.put("_id", shipmentPK);

        List<Document> newDocs = new ArrayList<>();

        for (int si = 0; si < itemsinshipment; si++) {
            Document shippedItem = recordFactory.getShipItem(custid, orderid, shipmentid, si);
            String shippedItemPK = String.format("C#%d#O%d#S#%d#I%d", shippedItem.getInteger("customerId"),
                    shippedItem.getInteger("orderId"),
                    shippedItem.getInteger("shipmentId"),
                    shippedItem.getInteger("shipmentItemId"));
            shippedItem.put("_id", shippedItemPK);
            newDocs.add(shippedItem);
        }

        newDocs.add(shipment); // This is at the end and they are ordered as thats the lightweight way to
        // ensure data
        // safeness - we might have orphaned shipItems but we won't have missing
        // shipItems. No TxN required to be safe

        rval = singleTable.insertMany(newDocs);
        return rval.getInsertedIds().size();
    }

    // Increase quantity for a single item already in the order
    public int updateSingleItem(int custid, int orderid, int itemid) {
        String orderPK = String.format("C#%d#O#%d#I#%d", custid, orderid, itemid);

        UpdateResult ur = singleTable.updateOne(Filters.eq("_id", orderPK), Updates.inc("qty", 1));
        return (int) ur.getModifiedCount();
    }

    // Simulates an operation that updates multiple bits of data
    // You may want to wrap this in a transaction too but as it has little
    // performance impact in this use case we won't for simplicity and clarity
    // Here we increase the count for an item but also update 'lastupdate' on the
    // order
    // Using bulkWrite to make it a single call to the server as it's the same
    // collection
    public int updateMultiItem(int custid, int orderid, int itemid) {
        String orderItemPK = String.format("C#%d#O#%d#I#%d", custid, orderid, itemid);
        String orderPK = String.format("C#%d#O#%d", custid, orderid);
        // Update The Item Quantity

        UpdateOneModel<Document> inc_item = new UpdateOneModel<>(Filters.eq("_id", orderItemPK),
                Updates.inc("qty", 1));
        UpdateOneModel<Document> set_order_lastdate = new UpdateOneModel<>(Filters.eq("_id", orderPK),
                Updates.set("lastupdate", new Date()));

        BulkWriteResult result = singleTable.bulkWrite(Arrays.asList(inc_item, set_order_lastdate));

        return result.getModifiedCount();
    }
    
    @Override
    public double[] executeMeasure(int opsToTest, String subtest, JSONObject args, boolean warmup){
        
        return switch (subtest) {
            case "GETORDERBYID" -> getOrdersByIdTest(opsToTest, args, warmup);
            case "ADDSHIPMENT" -> addShipmentsTest(opsToTest, args, warmup);
            case "INCITEMCOUNT" -> intItemCountTest(opsToTest, args, warmup);
            case "INCMULTIITEM" -> intItemCountTestWithDate(opsToTest, args, warmup);
            default -> null;
        };
    }
    
    private double[] getOrdersByIdTest(int opsToTest, JSONObject testOptions, boolean warmup) {

        int customers = ((Long)((JSONObject)testOptions.get("custom")).get("customers")).intValue();
        int orders = ((Long)((JSONObject)testOptions.get("custom")).get("orders")).intValue();
        
        double[] times = new double[opsToTest];
        
        for (int o = 0; o < opsToTest; o++) {
            int custid = random.nextInt(customers) + 1;
            int orderid = random.nextInt(orders) + 1;
            long startTime = System.nanoTime();
            getOrderById(custid, orderid);
            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000; // Microseconds
            times[o] = duration;
        }
        return times;
    }

    private double[] addShipmentsTest(int opsToTest, JSONObject testOptions, boolean warmup) {

        int customers = ((Long)((JSONObject)testOptions.get("custom")).get("customers")).intValue();
        int orders = ((Long)((JSONObject)testOptions.get("custom")).get("orders")).intValue();
        
        double[] times = new double[opsToTest];
        
        for (int o = 0; o < opsToTest; o++) {
            int custid = random.nextInt(customers) + 1;
            int orderid = random.nextInt(orders) + 1;
            long startTime = System.nanoTime();
            getOrderById(custid, orderid);
            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000; // Microseconds
            times[o] = duration;
        }
        return times;
    }

    private double[] intItemCountTest(int opsToTest, JSONObject testOptions, boolean warmup) {

        int customers = ((Long)((JSONObject)testOptions.get("custom")).get("customers")).intValue();
        int orders = ((Long)((JSONObject)testOptions.get("custom")).get("orders")).intValue();
        int items = ((Long)((JSONObject)testOptions.get("custom")).get("items")).intValue();
        
        double[] times = new double[opsToTest];
        
        for (int o = 0; o < opsToTest; o++) {
            int custid = random.nextInt(customers) + 1;
            int orderid = random.nextInt(orders) + 1;
            int itemid = random.nextInt(items / 2) + 1; // By selecting a random number up to
            // half items its more likely to be
            // there.
            long startTime = System.nanoTime();
            updateSingleItem(custid, orderid, itemid);
            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000; // Microseconds
            times[o] = duration;
        }
        return times;
    }

    private double[] intItemCountTestWithDate(int opsToTest, JSONObject testOptions, boolean warmup) {

        int customers = ((Long)((JSONObject)testOptions.get("custom")).get("customers")).intValue();
        int orders = ((Long)((JSONObject)testOptions.get("custom")).get("orders")).intValue();
        int items = ((Long)((JSONObject)testOptions.get("custom")).get("items")).intValue();
        
        double[] times = new double[opsToTest];
        
        for (int o = 0; o < opsToTest; o++) {
            int custid = random.nextInt(customers) + 1;
            int orderid = random.nextInt(orders) + 1;
            int itemid = random.nextInt(items / 2) + 1; // By selecting a random number up to
            // half items its more likely to be
            // there.
            long startTime = System.nanoTime();
            updateMultiItem(custid, orderid, itemid);
            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000; // Microseconds
            times[o] = duration;
        }
        return times;
    }

}
