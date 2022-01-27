package com.mongodb.jlp.orderbench;

import java.util.ArrayList;
import java.util.List;

import java.util.regex.Pattern;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.UpdateResult;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.bson.Document;
import org.bson.conversions.Bson;

public class MultiTableTest implements SchemaTest {

	Logger logger;
	MongoClient mongoClient;
	String[] types = { "warehouse", "customer", "product", "order", "orderitem", "invoice", "shipment",
			"shipmentitem" };
	String rawCollectionName = "data"; // TODO - pull thesee out to the TestOptions class
	String dbName = "orderbench";
	MongoDatabase db;
	ExecutorService executorService;
	RecordFactory recordFactory = new RecordFactory();

	// Single threaded version to start with
	// We can see if Multi thread helps later but more complex

	public List<Document> getOrderById(int customerId, int orderId) {
		String[] ordertypes = { "order", "orderitem", "invoice", "shipment",
				"shipmentitem" };

		String orderPrefix = String.format("C#%d#O#%d", customerId, orderId);

		List<Document> rval = new ArrayList<Document>();
		List<Future<List<Document>>> partials = new ArrayList<Future<List<Document>>>();

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

				e.printStackTrace();
			}
		}

		return rval;
	}

	/* Simulates N devices inserting X Documents */

	MultiTableTest(MongoClient m) {
		logger = LoggerFactory.getLogger(SingleTableTest.class);
		mongoClient = m;
		db = mongoClient.getDatabase(dbName);
		executorService = Executors.newFixedThreadPool(10);
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

	@Override
	public void cleanup() {
		executorService.shutdown();
	}

	@Override
	public void warmup() {
		String[] ordertypes = { "order", "orderitem", "invoice", "shipment", "shipmentitem" };

		for (String tname : ordertypes) {
			MongoCollection<Document> c = db.getCollection(tname);
			c.find(new Document("not", "true")).first(); // Collection scan will pull it all into cache if it can
			c.find(new Document("not", "true")).first(); // Collection scan will pull it all into cache if it can
		}

	}

	@Override
	// Greates a new shipment for all of the items in this order
	// Don't worry if they were already shipped - imagine they got lost
	// we only want to test the write so lets assume we already have the order doc
	// And select N items to ship where 1<N<NitemsMax

	public int addNewShipment(int custid, int orderid, int shipmentid, int itemsinshipment, int warehouseid) {

		int ndocs = 0;
		MongoCollection<Document> shipmentcollection = db.getCollection("shipment");
		MongoCollection<Document> shipmentitemcollection = db.getCollection("shipmentitem");

		Document shipment = recordFactory.getShipment(custid, orderid, shipmentid, warehouseid);

		String shipmentPK = String.format("C#%d#O%d#S#%d", shipment.getInteger("customerId"),
				shipment.getInteger("orderId"), shipment.getInteger("shipmentId"));
		shipment.put("_id", shipmentPK);

		List<Document> shipmentitems = new ArrayList<Document>();

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

	@Override
	public int updateSingleItem(int custid, int orderid, int itemid) {
		String orderPK = String.format("C#%d#O#%d#I#%d", custid, orderid, itemid);
		MongoCollection<Document> itemCollection = db.getCollection("orderitem");
		UpdateResult ur = itemCollection.updateOne(Filters.eq("_id", orderPK), Updates.inc("qty", 1));
		return (int) ur.getModifiedCount();
	}
}