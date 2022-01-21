package com.mongodb.jlp.orderbench;

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
		results = new ArrayList<Document>();
	}

	public List<Document> call() {
		collection.find(query).into(results);
		return results;
	}

}
