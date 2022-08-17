/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mongodb.devrel.pods.performancebench.models.multitable;

/**
 *
 * @author graeme
 */
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import com.mongodb.client.MongoCollection;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryWorker implements Callable<List<Document>> {

    Logger logger;
    MongoCollection<Document> collection;
    Bson query;
    List<Document> results;

    /* Simulates N devices inserting X Documents */

    QueryWorker(MongoCollection<Document> c, Bson q) {
        collection = c;

        logger = LoggerFactory.getLogger(QueryWorker.class);

        this.query = q;
        results = new ArrayList<>();
    }

    @Override
    public List<Document> call() {
        collection.find(query).into(results);
        return results;
    }

}
