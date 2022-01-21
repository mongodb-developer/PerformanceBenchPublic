package com.mongodb.jlp.orderbench;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import org.bson.Document;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleTableTest implements SchemaTest {

    Logger logger;
    MongoClient mongoClient;
    String rawCollectionName = "data"; // TODO - pull thesee out to the TestOptions class
    String dbName = "orderbench";
    MongoCollection<Document> singleTable;
    /* Simulates N devices inserting X Documents */

    SingleTableTest(MongoClient m) {
        logger = LoggerFactory.getLogger(SingleTableTest.class);
        mongoClient = m;
        singleTable = mongoClient.getDatabase(dbName).getCollection(rawCollectionName);
    }

    // Method to take the Bulk loaded data and prepare it for testing
    // For exmaple to reshape oir index it
    public void prepareTestData() {
        // For now we do nothing for the Single table test we just use as bulk loaded

    }

    // Order may be in multiple docs
    public List<Document> getOrderById(String orderId) {
        // Querying with _id field to avoid requiring index on OrderId - not sure that's
        // a good idea
        List<Document> rval = new ArrayList<Document>();
        Pattern regex = Pattern.compile(String.format("^%s[^0-9]", orderId));
        return singleTable.find(Filters.or(Filters.eq("_id", orderId), Filters.eq("_id", regex))).into(rval);
    }

    public String name() {
        return "SingleTableTest";
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub

    }

}