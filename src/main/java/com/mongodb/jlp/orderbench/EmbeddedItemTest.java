package com.mongodb.jlp.orderbench;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;

import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;

import com.mongodb.client.model.Filters;

public class EmbeddedItemTest implements SchemaTest {

	private static final String ORDER_COLLECTION = "order";
	private static final String INVOICE_COLLECTION = "invoice";
	// As MongoDB lets you use any collection or field name typos can be an isse
	// Put them in constants to avoid this
	private static final String EMBEDDED_COLLECTION_NAME = "embedded"; // TODO - pull these two out to the TestOptions
																		// class
	private static final String DB_NAME = "orderbench";
	private static final String SHIPMENT_COLLECTION = "shipment";
	private static final String SHIPMENT_ITEM_COLLECTION = "shipmentitem";
	private static final String SHIPMENT_ITEM_ID = "shipmentItemId";
	private static final String ORDERITEM = "orderitem";
	private static final String ORDER_ID = "orderId";
	private static final String CUSTOMER_ID = "customerId";
	private static final String SHIPMENT_ID = "shipmentId";
	Logger logger;
	MongoClient mongoClient;
	RecordFactory recordFactory = new RecordFactory();

	MongoCollection<Document> embeddedCollection;

	public List<Document> getOrderById(int customerId, int orderId) {
		List<Document> rval = new ArrayList<Document>();
		String orderPrefix = String.format("C#%d#O#%d", customerId, orderId);
		return embeddedCollection.find(Filters.eq("_id", orderPrefix)).into(rval);
	}

	EmbeddedItemTest(MongoClient m) {
		logger = LoggerFactory.getLogger(SingleTableTest.class);
		mongoClient = m;
		embeddedCollection = mongoClient.getDatabase(DB_NAME).getCollection(EMBEDDED_COLLECTION_NAME);
	}

	// Method to take the Bulk loaded data and prepare it for testing
	// For exmaple to reshape oir index it

	// TODO - Refactor all the below to use Aggregates.X and Filters.X

	public void prepareTestData() {

		logger.info("Creating Schema - 1 Collection , 1 Document per Order (Document Style)");
		logger.info("indexing Orderitems");

		mongoClient.getDatabase(DB_NAME).getCollection(ORDERITEM)
				.createIndex(Indexes.ascending(CUSTOMER_ID, ORDER_ID));

		logger.info("indexing invoices");

		mongoClient.getDatabase(DB_NAME).getCollection(INVOICE_COLLECTION)
				.createIndex(Indexes.ascending(CUSTOMER_ID, ORDER_ID));

		logger.info("indexing shipments");

		mongoClient.getDatabase(DB_NAME).getCollection(SHIPMENT_COLLECTION)
				.createIndex(Indexes.ascending(CUSTOMER_ID, ORDER_ID));

		logger.info("indexing shipmentitems");

		mongoClient.getDatabase(DB_NAME).getCollection(SHIPMENT_ITEM_COLLECTION)
				.createIndex(Indexes.ascending(CUSTOMER_ID, ORDER_ID, SHIPMENT_ITEM_ID));

		logger.info("Creating Embedded - This  may take a while");

		MongoDatabase database = mongoClient.getDatabase(DB_NAME);
		MongoCollection<Document> collection = database.getCollection(ORDER_COLLECTION);
		// Its easiuer to write complex aggregation by defining variables - even in the
		// shell

		Document GetAllOrders = new Document("$match", new Document("type", ORDER_COLLECTION));
		Document params = new Document(CUSTOMER_ID, "$" + CUSTOMER_ID).append(ORDER_ID, "$" + ORDER_ID);

		Document custIdEq = new Document("$eq", Arrays.asList("$" + CUSTOMER_ID, "$$" + CUSTOMER_ID));
		Document orderIdEq = new Document("$eq", Arrays.asList("$" + ORDER_ID, "$$" + ORDER_ID));
		Document orderExpression = new Document("$expr", new Document("$and", Arrays.asList(custIdEq, orderIdEq)));

		Document RedundantFieldRemoval = new Document("$project",
				Projections.exclude("_id", "type", CUSTOMER_ID, ORDER_ID));

		List<Bson> FilterByOrderPipeline = Arrays.asList(new Document("$match", orderExpression),
				RedundantFieldRemoval);

		Document GetOrderItems = new Document("$lookup", new Document("from", ORDERITEM).append("as", "items")
				.append("let", params).append("pipeline", FilterByOrderPipeline));

		Document GetOrderInvoices = new Document("$lookup",
				new Document("from", INVOICE_COLLECTION).append("as", "invoices")
						.append("let", params).append("pipeline", FilterByOrderPipeline));

		Document shipmentIdParam = new Document(SHIPMENT_ID, "$" + SHIPMENT_ID);
		Document shipmentIdEq = new Document("$eq", Arrays.asList("$" + SHIPMENT_ID, "$$" + SHIPMENT_ID));
		Document shipmentExpression = new Document("$expr",
				new Document("$and", Arrays.asList(custIdEq, orderIdEq, shipmentIdEq)));

		Bson RedundantFieldRemoval2 = new Document("$project",
				Projections.exclude("_id", "type", CUSTOMER_ID, ORDER_ID, SHIPMENT_ID));

		List<Bson> FilterByShipmentPipeline = Arrays.asList(new Document("$match", shipmentExpression),
				RedundantFieldRemoval2);

		Document GetItemsInShipment = new Document("$lookup",
				new Document("from", SHIPMENT_ITEM_COLLECTION).append("as", "items")
						.append("let", shipmentIdParam).append("pipeline", FilterByShipmentPipeline));

		Document ItemsInShipmentSimpleArray = new Document("$set",
				new Document("items", "$items.shipmentItemId"));
		List<Bson> getShipmentPipeline = Arrays.asList(new Document("$match", orderExpression), GetItemsInShipment,
				RedundantFieldRemoval, ItemsInShipmentSimpleArray);

		Document GetOrderShipments = new Document("$lookup",
				new Document("from", SHIPMENT_COLLECTION).append("as", "shipments")
						.append("let", params).append("pipeline", getShipmentPipeline));

		Document WriteResult = new Document("$out", EMBEDDED_COLLECTION_NAME);

		List<Document> pipeline = Arrays.asList(GetAllOrders, GetOrderItems, GetOrderShipments, GetOrderInvoices,
				WriteResult);

		logger.info(new Document("x", pipeline).toJson(JsonWriterSettings.builder().indent(true).build()));
		AggregateIterable<Document> result = collection.aggregate(pipeline);

		result.first();
		logger.info("Done!");
	}

	public String name() {
		return "EmbeddedItemsTest";
	}

	public void cleanup() {
		return;
	}

	public void warmup() {
		// Pull all into cache
		embeddedCollection.find(new Document("not", "true")).first();
		embeddedCollection.find(new Document("not", "true")).first();
	}

	@Override
	public int addNewShipment(int custid, int orderid, int shipmentid, int itemsinshipment, int warehouseid) {
		Document shipment = recordFactory.getShipment(custid, orderid, shipmentid, warehouseid);
		// Remove the fields we dont need
		shipment.remove("customerId");
		shipment.remove("orderId");
		shipment.remove("type");
		List<Integer> shipItems = new ArrayList<Integer>();

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

	@Override
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

	@Override
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