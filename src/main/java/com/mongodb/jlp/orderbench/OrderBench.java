package com.mongodb.jlp.orderbench;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.LogManager;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;

import org.apache.commons.cli.ParseException;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

public class OrderBench {
	static final String version = "OrderBench 0.0.1";
	static Logger logger;

	public static void main(String[] args) {
		LogManager.getLogManager().reset();

		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();
		TestOptions options = null;
		MongoClient mongoClient;

		logger = LoggerFactory.getLogger(OrderBench.class);
		logger.info(version);

		try {
			options = new TestOptions(args);
		} catch (ParseException e) {
			logger.error(e.getMessage());
			System.exit(1);
		}
		mongoClient = MongoClients.create(options.getString("uri"));
		// Quick check of connection up front
		Document pingResult = mongoClient.getDatabase("system").runCommand(new Document("hello", 1));
		logger.info(pingResult.toJson());

		RecordFactory factory = new RecordFactory();

		// No dynamic loading needed
		List<SchemaTest> tests = new ArrayList<SchemaTest>();
		tests.add(new SingleTableTest(mongoClient));
		tests.add(new MultiTableTest(mongoClient));
		tests.add(new EmbeddedItemTest(mongoClient));

		BulkLoad bulkLoad = new BulkLoad(mongoClient, factory);

		if (options.getBoolean("load", false)) {
			bulkLoad.loadInitialData(options);
			for (SchemaTest s : tests) {
				s.prepareTestData();
			}
		}

		/* Quick check things are working */
		logger.info("Quick self test - just to see we are getting results");
		int customer = 1;
		int order = 1;
		String orderId = String.format("C#%dO#%d", customer, order);
		for (SchemaTest s : tests) {
			List<Document> d = s.getOrderById(orderId);
			logger.info(s.name() + " Result size: " + d.size());
			if (d.size() == 0 || d.size() > 500) {
				logger.warn("DOES THIS LOOK CORRECT!!!");
			}
		}

		TestRunner runner = new TestRunner();
		logger.info("Running tests...");
		runner.runTest(tests, options);
		for (SchemaTest s : tests) {
			s.cleanup();
		}
	}

}
