package com.mongodb.jlp.orderbench;

import java.util.List;

import org.bson.Document;

public interface SchemaTest {
    void prepareTestData();

    String name();

    List<Document> getOrderById(String orderId);
}
