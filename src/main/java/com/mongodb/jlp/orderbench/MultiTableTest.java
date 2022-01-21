package com.mongodb.jlp.orderbench;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.bson.Document;

public class MultiTableTest implements SchemaTest {

    Logger logger;
    MongoClient mongoClient;
    String[] types = { "warehouse", "customer", "product", "order", "orderitem", "invoice", "shipment",
            "shipmentitem" };
    String rawCollectionName = "data"; // TODO - pull thesee out to the TestOptions class
    String dbName = "orderbench";
    MongoDatabase db;

    // Single threaded version to start with
    // We can see if Multi thread helps later but more complex

    public List<Document> getOrderById(String orderId) {
        String[] ordertypes = { "order", "orderitem", "invoice", "shipment",
                "shipmentitem" };

        List<Document> rval = new ArrayList<Document>();
        Pattern regex = Pattern.compile(String.format("^%s[^0-9]", orderId));
        for (String tname : ordertypes) {
            MongoCollection<Document> c = db.getCollection(tname);
            c.find(Filters.or(Filters.eq("_id", orderId), Filters.eq("_id", regex))).into(rval);
        }
        return rval;
    }

    /* Simulates N devices inserting X Documents */

    MultiTableTest(MongoClient m) {
        logger = LoggerFactory.getLogger(SingleTableTest.class);
        mongoClient = m;
        db = mongoClient.getDatabase(dbName);
    }

    // Method to take the Bulk loaded data and prepare it for testing
    // For exmaple to reshape oir index it
    public void prepareTestData() {
        // For now we do nothing for the Single table test we just use as bulk loaded
        logger.info("Creating Schema - 1 Collection per type (RDBMS style)");
        db.getCollection(rawCollectionName).createIndex(Indexes.ascending("type")); // TODO - pull out string constants
                                                                                    // for collection names

        for (String t : types) {
            logger.info("Writing " + t);
            // This is very old-school
            List<Document> pipeline = new ArrayList<Document>();

            pipeline.add(new Document("$match", new Document("type", t)));
            pipeline.add(new Document("$out", t));
            db.getCollection(rawCollectionName)
                    .aggregate(pipeline).first();
        }
    }

    public String name() {
        return "MultiTableTest";
    }

}