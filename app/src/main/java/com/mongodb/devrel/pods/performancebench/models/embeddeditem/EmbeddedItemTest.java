/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mongodb.devrel.pods.performancebench.models.embeddeditem;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.devrel.pods.performancebench.SchemaTest;
import com.mongodb.devrel.pods.performancebench.utilities.BulkLoadMDB;
import com.mongodb.devrel.pods.performancebench.utilities.RecordFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author graeme
 */
public class EmbeddedItemTest implements SchemaTest {
    
    Logger logger;
    MongoClient mongoClient;
    RecordFactory recordFactory = new RecordFactory();
    JSONObject args;
    JSONObject customArgs;
    private static final Random random = new Random();
    
    // As MongoDB lets you use any collection or field name typos can be an issue
    // Put them in constants to avoid this
    private static String EMBEDDED_COLLECTION_NAME;
    private static String RAW_COLLECTION_NAME;
    private static String DB_NAME;
    private static String SHIPMENT_DOCUMENTS;
    private static String SHIPMENT_ITEM_DOCUMENTS;
    private static String SHIPMENT_ITEM_ID;
    private static String ORDER_ITEM_DOCUMENTS;
    private static String ORDER_ID;
    private static String CUSTOMER_ID;
    private static String SHIPMENT_ID;
    private static String ORDER_DOCUMENTS;
    private static String INVOICE_DOCUMENTS;


    MongoCollection<Document> embeddedCollection;

    public EmbeddedItemTest() {
        logger = LoggerFactory.getLogger(EmbeddedItemTest.class);
    }
    
    
    @Override
    public void initialize(JSONObject newArgs){
        
        this.args = newArgs;
        this.customArgs = (JSONObject)args.get("custom");
                
        mongoClient = MongoClients.create(customArgs.get("uri").toString());
        // Quick check of connection up front
        Document pingResult = mongoClient.getDatabase("system").runCommand(new Document("hello", 1));
        logger.debug(pingResult.toJson());
        
        EMBEDDED_COLLECTION_NAME = (String)customArgs.get("embedded_collection_name");
        RAW_COLLECTION_NAME = (String)customArgs.get("collectionName");
        DB_NAME = (String)customArgs.get("db_name");
        SHIPMENT_DOCUMENTS = (String)customArgs.get("shipment_type");
        SHIPMENT_ITEM_DOCUMENTS = (String)customArgs.get("shipment_item_type");
        SHIPMENT_ITEM_ID = (String)customArgs.get("shipment_item_id");
        ORDER_ITEM_DOCUMENTS = (String)customArgs.get("orderitem_type");
        ORDER_ID = (String)customArgs.get("order_id");
        CUSTOMER_ID = (String)customArgs.get("customer_id");
        SHIPMENT_ID = (String)customArgs.get("shipment_id");
        ORDER_DOCUMENTS = (String)customArgs.get("order_type");
        INVOICE_DOCUMENTS = (String)customArgs.get("invoice_type");
        
        embeddedCollection = mongoClient.getDatabase(DB_NAME).getCollection(EMBEDDED_COLLECTION_NAME);

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
        List<Document> rval = new ArrayList<>();
        String orderPrefix = String.format("C#%d#O#%d", customerId, orderId);
        return embeddedCollection.find(Filters.eq("_id", orderPrefix)).into(rval);
    }

    // Method to take the Bulk loaded data and prepare it for testing
    // For exmaple to reshape oir index it
    // TODO - Refactor all the below to use Aggregates.X and Filters.X
    public void prepareTestData() {
        
        logger.debug("Creating Schema - 1 Collection , 1 Document per Order (Document Style)");
        logger.debug("Adding Indexes");

        mongoClient.getDatabase(DB_NAME).getCollection(RAW_COLLECTION_NAME)
                .createIndex(Indexes.ascending(CUSTOMER_ID, ORDER_ID, SHIPMENT_ITEM_ID));

        logger.debug("Creating Embedded - This  may take a while");

        MongoDatabase database = mongoClient.getDatabase(DB_NAME);
        MongoCollection<Document> collection = database.getCollection(RAW_COLLECTION_NAME);
        // Its easiuer to write complex aggregation by defining variables - even in the
        // shell

        Document GetAllOrders = new Document("$match", new Document("type", ORDER_DOCUMENTS));
        Document params = new Document(CUSTOMER_ID, "$" + CUSTOMER_ID).append(ORDER_ID, "$" + ORDER_ID);

        Document custIdEq = new Document("$eq", Arrays.asList("$" + CUSTOMER_ID, "$$" + CUSTOMER_ID));
        Document orderIdEq = new Document("$eq", Arrays.asList("$" + ORDER_ID, "$$" + ORDER_ID));
        Document orderItemTypeEq = new Document("$eq", Arrays.asList("$type", ORDER_ITEM_DOCUMENTS));
        
        Document orderExpression = new Document("$expr", new Document("$and", Arrays.asList(custIdEq, orderIdEq, orderItemTypeEq)));

        Document RedundantFieldRemoval = new Document("$project",
                Projections.exclude("_id", "type", CUSTOMER_ID, ORDER_ID));

        List<Bson> FilterByOrderPipeline = Arrays.asList(new Document("$match", orderExpression),
                RedundantFieldRemoval);

        Document GetOrderItems = new Document("$lookup", new Document("from", RAW_COLLECTION_NAME).append("as", "items")
                .append("let", params).append("pipeline", FilterByOrderPipeline));

        
        
        Document invoiceTypeEq = new Document("$eq", Arrays.asList("$type", INVOICE_DOCUMENTS));
        Document invoiceExpression = new Document("$expr", new Document("$and", Arrays.asList(custIdEq, orderIdEq, invoiceTypeEq)));

        List<Bson> FilterByInvoicePipeline = Arrays.asList(new Document("$match", invoiceExpression),
                RedundantFieldRemoval);
        
        
        Document GetOrderInvoices = new Document("$lookup",
                new Document("from", RAW_COLLECTION_NAME).append("as", "invoices")
                        .append("let", params).append("pipeline", FilterByInvoicePipeline));

        Document shipmentIdParam = new Document(SHIPMENT_ID, "$" + SHIPMENT_ID);
        Document shipmentIdEq = new Document("$eq", Arrays.asList("$" + SHIPMENT_ID, "$$" + SHIPMENT_ID));
        Document shipmentItemTypeEq = new Document("$eq", Arrays.asList("$type", SHIPMENT_DOCUMENTS));
        Document shipmentExpression = new Document("$expr",
                new Document("$and", Arrays.asList(custIdEq, orderIdEq, shipmentIdEq, shipmentItemTypeEq)));

        Bson RedundantFieldRemoval2 = new Document("$project",
                Projections.exclude("_id", "type", CUSTOMER_ID, ORDER_ID, SHIPMENT_ID));

        List<Bson> FilterByShipmentPipeline = Arrays.asList(new Document("$match", shipmentExpression),
                RedundantFieldRemoval2);
        
        Document GetItemsInShipment = new Document("$lookup",
                new Document("from", RAW_COLLECTION_NAME).append("as", "items")
                        .append("let", shipmentIdParam).append("pipeline", FilterByShipmentPipeline));

        Document ItemsInShipmentSimpleArray = new Document("$set",
                new Document("items", "$items.shipmentItemId"));
        List<Bson> getShipmentPipeline = Arrays.asList(new Document("$match", orderExpression), GetItemsInShipment,
                RedundantFieldRemoval, ItemsInShipmentSimpleArray);
        
        Document GetOrderShipments = new Document("$lookup",
                new Document("from", RAW_COLLECTION_NAME).append("as", "shipments")
                        .append("let", params).append("pipeline", getShipmentPipeline));

        Document WriteResult = new Document("$out", EMBEDDED_COLLECTION_NAME);

        List<Document> pipeline = Arrays.asList(GetAllOrders, GetOrderItems, GetOrderShipments, GetOrderInvoices,
                WriteResult);

        logger.debug(new Document("x", pipeline).toJson(JsonWriterSettings.builder().indent(true).build()));
        AggregateIterable<Document> result = collection.aggregate(pipeline);

        result.first();
        logger.debug("Done!");
    }

    @Override
    public String name() {
        return "EmbeddedItemsTest";
    }

    @Override
    public void cleanup() {
        return;
    }

    @Override
    public void warmup() {
        // Pull all into cache
        embeddedCollection.find(new Document("not", "true")).first();
        embeddedCollection.find(new Document("not", "true")).first();
    }

    public int addNewShipment(int custid, int orderid, int shipmentid, int itemsinshipment, int warehouseid) {
        Document shipment = recordFactory.getShipment(custid, orderid, shipmentid, warehouseid);
        // Remove the fields we dont need
        shipment.remove("customerId");
        shipment.remove("orderId");
        shipment.remove("type");
        List<Integer> shipItems = new ArrayList<>();

        for (int si = 0; si < itemsinshipment; si++) {
            Document shippedItem = recordFactory.getShipItem(custid, orderid, shipmentid, si);
            // shippedItem.remove("customerId");
            // shippedItem.remove("orderId");
            // shippedItem.remove("type");
            // shippedItem.remove("shipmentId");
            shipItems.add(shippedItem.getInteger("shipmentItemId"));
        }
        shipment.append("items", shipItems);
        UpdateResult r = embeddedCollection.updateOne(Filters.eq("_id", String.format("C#%d#O#%d", custid, orderid)),
                Updates.push("shipments", shipment));
        return (int) r.getModifiedCount();
    }

    public int updateSingleItem(int custid, int orderid, int itemid) {
        String orderPK = String.format("C#%d#O#%d", custid, orderid); // Lookup on PK is fine - no real need for index
        // on
        // Item
        // But we do put Item in the Query to help us use $ to identify which to modify

        UpdateResult ur = embeddedCollection.updateOne(
                Filters.and(Filters.eq("_id", orderPK), Filters.eq("items.itemId", itemid)),
                Updates.inc("items.$.qty", 1));
        return (int) ur.getModifiedCount();

    }

    public int updateMultiItem(int custid, int orderid, int itemid) {
        String orderPK = String.format("C#%d#O#%d", custid, orderid); // Lookup on PK is fine - no real need for index
        // on
        // Item
        // But we do put Item in the Query to help us use $ to identify which to modify

        UpdateResult ur = embeddedCollection.updateOne(
                Filters.and(Filters.eq("_id", orderPK), Filters.eq("items.itemId", itemid)),
                Updates.combine(Updates.inc("items.$.qty", 1), Updates.set("lastupdate", new Date())));
        return (int) ur.getModifiedCount();
    }

}
