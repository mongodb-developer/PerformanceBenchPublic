package com.mongodb.jlp.orderbench;

import java.util.List;

import org.bson.Document;

public interface SchemaTest {
    void prepareTestData();

    String name();

    void cleanup();

    List<Document> getOrderById(String orderId);
}
