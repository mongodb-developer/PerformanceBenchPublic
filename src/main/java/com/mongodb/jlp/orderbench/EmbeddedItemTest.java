package com.mongodb.jlp.orderbench;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.bson.Document;
import com.mongodb.client.model.Filters;

public class EmbeddedItemTest implements SchemaTest {

        Logger logger;
        MongoClient mongoClient;
        String rawCollectionName = "data"; // TODO - pull thesee out to the TestOptions class
        String embeddedCollectionName = "embedded"; // TODO - pull thesee out to the TestOptions class
        String dbName = "orderbench";
        MongoCollection<Document> embeddedCollection;

        public List<Document> getOrderById(String orderId) {
                List<Document> rval = new ArrayList<Document>();

                return embeddedCollection.find(Filters.eq("_id", orderId)).into(rval);
        }

        EmbeddedItemTest(MongoClient m) {
                logger = LoggerFactory.getLogger(SingleTableTest.class);
                mongoClient = m;
                embeddedCollection = mongoClient.getDatabase(dbName).getCollection(embeddedCollectionName);
        }

        // Method to take the Bulk loaded data and prepare it for testing
        // For exmaple to reshape oir index it
        public void prepareTestData() {
                // For now we do nothing for the Single table test we just use as bulk loaded
                logger.info("Creating Schema - 1 Collection , 1 Document per Order (Document Style)");
                logger.info("indexing Orderitems");

                mongoClient.getDatabase("orderbench").getCollection("orderitem")
                                .createIndex(Indexes.ascending("customerId", "orderId"));

                logger.info("indexing invoices");

                mongoClient.getDatabase("orderbench").getCollection("invoice")
                                .createIndex(Indexes.ascending("customerId", "orderId"));

                logger.info("indexing shipments");

                mongoClient.getDatabase("orderbench").getCollection("shipment")
                                .createIndex(Indexes.ascending("customerId", "orderId"));

                logger.info("Creating Embedded - This  may take a while");

                MongoDatabase database = mongoClient.getDatabase("orderbench");
                MongoCollection<Document> collection = database.getCollection("order");

                // This is not how I would normally write this - but I wrote it in variable form
                // in JS
                // Took the output and passed it through Compass to the single Object Java
                // version
                // Sory for the bad coding form here.

                AggregateIterable<Document> result = collection.aggregate(Arrays.asList(new Document("$match",
                                new Document("type", "order")),
                                new Document("$lookup",
                                                new Document("from", "orderitem")
                                                                .append("let",
                                                                                new Document("customerId",
                                                                                                "$customerId")
                                                                                                                .append("orderId",
                                                                                                                                "$orderId"))
                                                                .append("pipeline", Arrays.asList(new Document("$match",
                                                                                new Document("$expr",
                                                                                                new Document("$and",
                                                                                                                Arrays.asList(
                                                                                                                                new Document("$eq",
                                                                                                                                                Arrays.asList("$customerId",
                                                                                                                                                                "$$customerId")),
                                                                                                                                new Document("$eq",
                                                                                                                                                Arrays.asList("$orderId",
                                                                                                                                                                "$$orderId"))))))))
                                                                .append("as", "orders")),
                                new Document("$lookup",
                                                new Document("from", "invoice")
                                                                .append("let",
                                                                                new Document("customerId",
                                                                                                "$customerId")
                                                                                                                .append("orderId",
                                                                                                                                "$orderId"))
                                                                .append("pipeline", Arrays.asList(new Document("$match",
                                                                                new Document("$expr",
                                                                                                new Document("$and",
                                                                                                                Arrays.asList(
                                                                                                                                new Document("$eq",
                                                                                                                                                Arrays.asList("$customerId",
                                                                                                                                                                "$$customerId")),
                                                                                                                                new Document("$eq",
                                                                                                                                                Arrays.asList("$orderId",
                                                                                                                                                                "$$orderId"))))))))
                                                                .append("as", "invoice")),
                                new Document("$lookup",
                                                new Document("from", "shipment")
                                                                .append("let",
                                                                                new Document("customerId",
                                                                                                "$customerId")
                                                                                                                .append("orderId",
                                                                                                                                "$orderId"))
                                                                .append("pipeline", Arrays.asList(new Document("$match",
                                                                                new Document("$expr",
                                                                                                new Document("$and",
                                                                                                                Arrays.asList(
                                                                                                                                new Document("$eq",
                                                                                                                                                Arrays.asList("$customerId",
                                                                                                                                                                "$$customerId")),
                                                                                                                                new Document("$eq",
                                                                                                                                                Arrays.asList("$orderId",
                                                                                                                                                                "$$orderId"))))))))
                                                                .append("as", "shipments")),
                                new Document("$out", "embedded")));

                result.first();
                logger.info("Done!");
        }

        public String name() {
                return "EmbeddedItemsTest";
        }

        @Override
        public void cleanup() {
                // TODO Auto-generated method stub

        }

}