package com.mongodb.jlp.orderbench;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//This class takes a List of test types and runs them
//NEEDS SPLIT IN TWO!

public class TestRunner implements Runnable {

	Logger logger;
	private List<SchemaTest> tests;
	private TestOptions options;
	private static Random random = new Random();
	ExecutorService executorService;
	SchemaTest test;
	boolean warmup;

	String[] subtests = {
			"GETORDERBYID", "ADDSHIPMENT", "INCITEMCOUNT"
	};

	int OPS_TO_TEST;
	double[] times;
	private String subtest;

	/* Simulates N devices inserting X Documents */

	TestRunner() {
		logger = LoggerFactory.getLogger(TestRunner.class);
	}

	void prep(SchemaTest t, TestOptions o, boolean warmup, String st) {
		options = o;
		test = t;
		this.warmup = warmup;
		this.subtest = st;
		OPS_TO_TEST = options.getInteger("itterations");
		times = new double[OPS_TO_TEST];
	}

	void runTest(List<SchemaTest> tests, TestOptions options) {

		final int NTHREADS = options.getInteger("threads", 20); /* Optimal here is actually a key tunable */
		OPS_TO_TEST = options.getInteger("itterations");
		for (SchemaTest s : tests) {

			logger.info("Warmup");
			s.warmup();

			logger.info("Testing " + OPS_TO_TEST);

			for (String st : subtests) {
				executorService = Executors.newFixedThreadPool(NTHREADS);
				List<TestRunner> runners = new ArrayList<TestRunner>();

				for (int t = 0; t < NTHREADS; t++) {
					TestRunner tr = new TestRunner();
					tr.prep(s, options, false, st);
					runners.add(tr);
					executorService.execute(tr); // calls run()
				}
				long startTime = System.nanoTime();

				executorService.shutdown();
				try {
					executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
				} catch (InterruptedException e) {
				}

				long endTime = System.nanoTime();

				double[] alltimes = new double[OPS_TO_TEST * NTHREADS];
				int i = 0;
				for (TestRunner t : runners) {
					System.arraycopy(t.times, 0, alltimes, i, OPS_TO_TEST);
					i += OPS_TO_TEST;
				}

				Arrays.sort(alltimes);
				int timelen = alltimes.length;
				long total = 0;
				for (int t = 0; t < timelen; t++) {
					total += alltimes[t];
				}

				double secs = (double) (endTime - startTime) / 1000000000.0;

				logger.info(String.format(
						"%s:%s max(ms): %.2f min(ms): %.2f mean(ms): %.2f 95th centile(ms): %.2f throughput(qps): %.1f",
						s.name(), st,
						alltimes[timelen - 1] / 1000.0,
						alltimes[0] / 1000.0,
						(total / 1000.0) / timelen,
						alltimes[(int) (timelen * .95)] / 1000.0,
						(double) timelen / secs));
			}

		}
	}

	void getOrdersByIdTest(SchemaTest s, TestOptions testOptions, boolean warmup) {

		for (int o = 0; o < OPS_TO_TEST; o++) {
			int custid = random.nextInt(testOptions.getInteger("customers")) + 1;
			int orderid = random.nextInt(testOptions.getInteger("orders")) + 1;
			long startTime = System.nanoTime();
			s.getOrderById(custid, orderid);
			long endTime = System.nanoTime();
			long duration = (endTime - startTime) / 1000; // Microseconds
			times[o] = duration;
		}
	}

	void addShipmentsTest(SchemaTest s, TestOptions testOptions, boolean warmup) {

		for (int o = 0; o < OPS_TO_TEST; o++) {
			int custid = random.nextInt(testOptions.getInteger("customers")) + 1;
			int orderid = random.nextInt(testOptions.getInteger("orders")) + 1;
			long startTime = System.nanoTime();
			s.getOrderById(custid, orderid);
			long endTime = System.nanoTime();
			long duration = (endTime - startTime) / 1000; // Microseconds
			times[o] = duration;
		}
	}

	void intItemCountTest(SchemaTest s, TestOptions testOptions, boolean warmup) {

		for (int o = 0; o < OPS_TO_TEST; o++) {
			int custid = random.nextInt(testOptions.getInteger("customers")) + 1;
			int orderid = random.nextInt(testOptions.getInteger("orders")) + 1;
			int itemid = random.nextInt(testOptions.getInteger("items") / 2) + 1; // By selecting a random number up to
																					// half items its more likely to be
																					// there.
			long startTime = System.nanoTime();
			s.updateSingleItem(custid, orderid, itemid);
			long endTime = System.nanoTime();
			long duration = (endTime - startTime) / 1000; // Microseconds
			times[o] = duration;
		}
	}

	@Override
	public void run() {
		// Change this so we can specify what test it should launch.
		switch (subtest) {
			case "GETORDERBYID":
				getOrdersByIdTest(test, options, warmup);
				break;
			case "ADDSHIPMENT":
				addShipmentsTest(test, options, warmup);
				break;
			case "INCITEMCOUNT":
				intItemCountTest(test, options, warmup);
				break;
			default:
				break;
		}
	}
}
