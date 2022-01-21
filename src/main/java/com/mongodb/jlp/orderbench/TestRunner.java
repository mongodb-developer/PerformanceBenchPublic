package com.mongodb.jlp.orderbench;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//This class takes a List of test types and runs them

public class TestRunner {

	Logger logger;
	private static Random random = new Random();
	/* Simulates N devices inserting X Documents */

	TestRunner() {
		logger = LoggerFactory.getLogger(TestRunner.class);
	}

	void runTest(List<SchemaTest> tests, TestOptions options) {
		logger.info("Warmup");
		for (SchemaTest s : tests) {
			getOrdersByIdTest(s, options, true);
		}
		logger.info("Test");
		for (SchemaTest s : tests) {
			getOrdersByIdTest(s, options, false);
		}
	}

	void getOrdersByIdTest(SchemaTest s, TestOptions testOptions, boolean warmup) {
		int ORDERSTOTEST = 50000;
		int custid = random.nextInt(testOptions.getInteger("customers"));
		int orderid = random.nextInt(testOptions.getInteger("orders"));

		double[] times = new double[ORDERSTOTEST];
		long total = 0;
		for (int o = 0; o < ORDERSTOTEST; o++) {
			String orderId = String.format("C#%dO#%d", custid, orderid);
			long startTime = System.nanoTime();
			s.getOrderById(orderId);
			long endTime = System.nanoTime();
			long duration = (endTime - startTime) / 1000; // Microseconds
			times[o] = duration;
			total += duration;
		}
		if (!warmup) {
			Arrays.sort(times);
			logger.info(String.format("%s max(ms): %.2f min(ms): %.2f mean(ms): %.2f 95th centile(ms): %.2f", s.name(),
					times[ORDERSTOTEST - 1] / 1000.0, times[0] / 1000.0, (total / 1000.0) / ORDERSTOTEST,
					times[(int) (ORDERSTOTEST * .95)] / 1000.0));
		}
	}

}
