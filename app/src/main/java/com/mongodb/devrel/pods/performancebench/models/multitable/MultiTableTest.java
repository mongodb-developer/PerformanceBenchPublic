/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mongodb.devrel.pods.performancebench.models.multitable;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.devrel.pods.performancebench.SchemaTest;
import com.mongodb.devrel.pods.performancebench.utilities.BulkLoadMDB;
import com.mongodb.devrel.pods.performancebench.utilities.RecordFactory;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author graeme
 */
public class MultiTableTest implements SchemaTest {
    
    Logger logger;
    MongoClient mongoClient;
    MongoCollection<Document> singleTable;
    RecordFactory recordFactory = new RecordFactory();
    JSONObject args;
    JSONObject customArgs;
    private static final Random random = new Random();
    String[] collectionTypes = { "warehousescoll", "customerscoll", "productscoll", "orderscoll", "orderitemscoll", 
        "invoicescoll", "shipmentscoll", "shipmentitemscoll" };
    MongoDatabase db;
    ExecutorService executorService;

    
    public MultiTableTest() {
        logger = LoggerFactory.getLogger(MultiTableTest.class);
    }
    
    
    @Override
    public void initialize(JSONObject newArgs){
        
        this.args = newArgs;
        this.customArgs = (JSONObject)args.get("custom");
                
        mongoClient = MongoClients.create(customArgs.get("uri").toString());
        // Quick check of connection up front
        Document pingResult = mongoClient.getDatabase("system").runCommand(new Document("hello", 1));
        logger.debug(pingResult.toJson());
        
        db = mongoClient.getDatabase((String)customArgs.get("dbname"));
	executorService = Executors.newFixedThreadPool(10);
        
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
  
    public List<Document> getOrderById(int customerId, int orderId) {
        
        String[] ordertypes = {"order", "orderitem", "invoice", "shipment",
            "shipmentitem"};

        String orderPrefix = String.format("C#%d#O#%d", customerId, orderId);

        List<Document> rval = new ArrayList<>();
        List<Future<List<Document>>> partials = new ArrayList<>();

        for (String tname : ordertypes) {
            MongoCollection<Document> c = db.getCollection(tname);
            // Querying with _id field to avoid requiring index on OrderId - not sure that's
            // a good idea from a readability perspective but Rick H wanted to try that
            // so the query is Orderid <= _id <= Orderid + "$" (as we want orderid or
            // orderid#....)
            // Secondary indexes are fine in MongoDB :-)

            Bson query = Filters.and(Filters.gte("_id", orderPrefix), Filters.lt("_id", orderPrefix + "$"));
            Future<List<Document>> future = executorService.submit(new QueryWorker(c, query));
            partials.add(future);
        }
        for (Future<List<Document>> p : partials) {
            try {
                rval.addAll(p.get());
            } catch (InterruptedException | ExecutionException e) {
                logger.error("Error running MultiTableTest getOrderById");
                logger.error(e.getMessage());
                System.exit(1);
                return null;
            }
        }

        return rval;
    }

    // Method to take the Bulk loaded data and prepare it for testing
    // For exmaple to reshape oir index it
    public void prepareTestData() {
        logger.debug("Creating Schema - 1 Collection per type (RDBMS style)");
        db.getCollection(customArgs.get("collectionName").toString()).createIndex(Indexes.ascending("type")); // TODO - pull out string constants
        // for collection names

        for (String t : collectionTypes) {
            logger.debug("Writing " + t);
            // This is very old-school
            List<Document> pipeline = new ArrayList<>();

            pipeline.add(new Document("$match", new Document("type", customArgs.get(t).toString())));
            pipeline.add(new Document("$out", customArgs.get(t).toString()));
            db.getCollection(customArgs.get("collectionName").toString())
                    .aggregate(pipeline).first();
        }
    }

    @Override
    public String name() {
        return "MultiTableTest";
    }

    @Override
    public void cleanup() {
        mongoClient.close();
        executorService.shutdown();
    }

    @Override
    public void warmup() {
        String[] ordertypes = {"orderscoll", "orderitemscoll", "invoicescoll", "shipmentscoll", "shipmentitemscoll"};

        for (String tname : ordertypes) {
            MongoCollection<Document> c = db.getCollection(customArgs.get(tname).toString());
            c.find(new Document("not", "true")).first(); // Collection scan will pull it all into cache if it can
            c.find(new Document("not", "true")).first(); // Collection scan will pull it all into cache if it can
        }
    }

    public int addNewShipment(int custid, int orderid, int shipmentid, int itemsinshipment, int warehouseid) {

        int ndocs;
        MongoCollection<Document> shipmentcollection = db.getCollection(customArgs.get("shipmentscoll").toString());
        MongoCollection<Document> shipmentitemcollection = db.getCollection(customArgs.get("shipmentitemscoll").toString());

        Document shipment = recordFactory.getShipment(custid, orderid, shipmentid, warehouseid);

        String shipmentPK = String.format("C#%d#O%d#S#%d", shipment.getInteger("customerId"),
                shipment.getInteger("orderId"), shipment.getInteger("shipmentId"));
        shipment.put("_id", shipmentPK);

        List<Document> shipmentitems = new ArrayList<>();

        for (int si = 0; si < itemsinshipment; si++) {
            Document shippedItem = recordFactory.getShipItem(custid, orderid, shipmentid, si);
            String shippedItemPK = String.format("C#%d#O%d#S#%d#I%d", shippedItem.getInteger("customerId"),
                    shippedItem.getInteger("orderId"),
                    shippedItem.getInteger("shipmentId"),
                    shippedItem.getInteger("shipmentItemId"));
            shippedItem.put("_id", shippedItemPK);
            shipmentitems.add(shippedItem);
        }

        ndocs = shipmentitemcollection.insertMany(shipmentitems).getInsertedIds().size();
        ndocs += shipmentcollection.insertOne(shipment).wasAcknowledged() ? 1 : 0;
        return ndocs;
    }

    public int updateSingleItem(int custid, int orderid, int itemid) {
        String orderPK = String.format("C#%d#O#%d#I#%d", custid, orderid, itemid);
        MongoCollection<Document> itemCollection = db.getCollection("orderitem");
        UpdateResult ur = itemCollection.updateOne(Filters.eq("_id", orderPK), Updates.inc("qty", 1));
        return (int) ur.getModifiedCount();
    }

    public int updateMultiItem(int custid, int orderid, int itemid) {
        String orderItemPK = String.format("C#%d#O#%d#I#%d", custid, orderid, itemid);
        String orderPK = String.format("C#%d#O#%d", custid, orderid);
        // Update The Item Quantity
        MongoCollection<Document> itemCollection = db.getCollection("orderitem");
        UpdateResult ur = itemCollection.updateOne(Filters.eq("_id", orderItemPK), Updates.inc("qty", 1));
        // Update the main order record which kees a track of the last change
        MongoCollection<Document> orderCollection = db.getCollection("order");
        UpdateResult ur2 = orderCollection.updateOne(Filters.eq("_id", orderPK), Updates.set("lastupdate", new Date()));

        return (int) ur.getModifiedCount() + (int) ur2.getModifiedCount();
    }

}
