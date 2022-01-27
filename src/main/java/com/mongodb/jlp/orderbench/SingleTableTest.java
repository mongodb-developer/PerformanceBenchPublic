package com.mongodb.jlp.orderbench;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.UpdateResult;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleTableTest implements SchemaTest {

	Logger logger;
	MongoClient mongoClient;
	String rawCollectionName = "data"; // TODO - pull thesee out to the TestOptions class
	String dbName = "orderbench";
	MongoCollection<Document> singleTable;
	RecordFactory recordFactory = new RecordFactory();
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
	public List<Document> getOrderById(int customerId, int orderId) {
		List<Document> rval = new ArrayList<Document>();
		String orderPrefix = String.format("C#%d#O#%d", customerId, orderId);
		// Querying with _id field to avoid requiring index on OrderId - not sure that's
		// a good idea from a readability perspective but Rick H wanted to try that
		// so the query is Orderid <= _id <= Orderid + "$" (as we want orderid or
		// orderid#....)
		// Secondary indexes are fine in MongoDB :-)

		Bson query = Filters.and(Filters.gte("_id", orderPrefix), Filters.lt("_id", orderPrefix + "$"));
		return singleTable.find(query).into(rval);
	}

	// Greates a new shipment for all of the items in this order
	// Don't worry if they were already shipped - imagine they got lost
	// we only want to test the write so lets assume we already have the order doc
	// And select N items to ship where 1<N<NitemsMax

	public int addNewShipment(int custid, int orderid, int shipmentid, int itemsinshipment, int warehouseid) {
		InsertManyResult rval;

		Document shipment = recordFactory.getShipment(custid, orderid, shipmentid, warehouseid);

		String shipmentPK = String.format("C#%d#O%d#S#%d", shipment.getInteger("customerId"),
				shipment.getInteger("orderId"), shipment.getInteger("shipmentId"));

		shipment.put("_id", shipmentPK);

		List<Document> newDocs = new ArrayList<Document>();

		for (int si = 0; si < itemsinshipment; si++) {
			Document shippedItem = recordFactory.getShipItem(custid, orderid, shipmentid, si);
			String shippedItemPK = String.format("C#%d#O%d#S#%d#I%d", shippedItem.getInteger("customerId"),
					shippedItem.getInteger("orderId"),
					shippedItem.getInteger("shipmentId"),
					shippedItem.getInteger("shipmentItemId"));
			shippedItem.put("_id", shippedItemPK);
			newDocs.add(shippedItem);
		}

		newDocs.add(shipment); // This is at the end and they are ordered as thats the lightweight way to
								// ensure data
		// safeness - we might have orphaned shipItems but we won't have missing
		// shipItems. No TxN required to be safe

		rval = singleTable.insertMany(newDocs);
		return rval.getInsertedIds().size();
	}

	public String name() {
		return "SingleTableTest";
	}

	public void warmup() {
		singleTable.find(new Document("not", "true")).first(); // Collection scan will pull it all into cache if it can
		singleTable.find(new Document("not", "true")).first(); // Collection scan will pull it all into cache if it can
		// If it doesnt fit then we will part fill the cache;

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	// Increase quantity for a single item already in the order
	@Override
	public int updateSingleItem(int custid, int orderid, int itemid) {
		String orderPK = String.format("C#%d#O#%d#I#%d", custid, orderid, itemid);

		UpdateResult ur = singleTable.updateOne(Filters.eq("_id", orderPK), Updates.inc("qty", 1));
		return (int) ur.getModifiedCount();
	}

}