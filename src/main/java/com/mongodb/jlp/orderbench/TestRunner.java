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

public class TestRunner implements Runnable {

	Logger logger;
	private List<SchemaTest> tests;
	private TestOptions options;
	private static Random random = new Random();
	ExecutorService executorService;
	SchemaTest test;
	boolean warmup;
	int ORDERSTOTEST = 50000;
	double[] times = new double[ORDERSTOTEST];

	/* Simulates N devices inserting X Documents */

	TestRunner() {
		logger = LoggerFactory.getLogger(TestRunner.class);
	}

	void prep(SchemaTest t, TestOptions o, boolean warmup) {
		options = o;
		test = t;
		this.warmup = warmup;
	}

	void runTest(List<SchemaTest> tests, TestOptions options) {

		final int NTHREADS = options.getInteger("threads", 20); /* Optimal here is actually a key tunable */

		for (SchemaTest s : tests) {
			executorService = Executors.newFixedThreadPool(NTHREADS);
			logger.info("Warmup");
			prep(s, options, true);
			run();
			logger.info("Test");
			List<TestRunner> runners = new ArrayList<TestRunner>();

			for (int t = 0; t < NTHREADS; t++) {
				TestRunner tr = new TestRunner();
				tr.prep(s, options, false);
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
			double[] alltimes = new double[ORDERSTOTEST * NTHREADS];
			int i = 0;
			for (TestRunner t : runners) {
				System.arraycopy(t.times, 0, alltimes, i, ORDERSTOTEST);
				i += ORDERSTOTEST;
			}
			Arrays.sort(alltimes);
			int timelen = alltimes.length;
			long total = 0;
			for (int t = 0; t < timelen; t++) {
				total += alltimes[t];
			}

			double secs = (double) (endTime - startTime) / 1000000000.0;

			logger.info(String.format(
					"%s max(ms): %.2f min(ms): %.2f mean(ms): %.2f 95th centile(ms): %.2f throughput(qps): %.1f",
					s.name(),
					alltimes[timelen - 1] / 1000.0,
					alltimes[0] / 1000.0,
					(total / 1000.0) / timelen,
					alltimes[(int) (timelen * .95)] / 1000.0,
					(double) timelen / secs));
			// Gather up the Stats rather then printing and merge them.
		}
	}

	void getOrdersByIdTest(SchemaTest s, TestOptions testOptions, boolean warmup) {

		int custid = random.nextInt(testOptions.getInteger("customers"));
		int orderid = random.nextInt(testOptions.getInteger("orders"));

		for (int o = 0; o < ORDERSTOTEST; o++) {
			String orderId = String.format("C#%dO#%d", custid, orderid);
			long startTime = System.nanoTime();
			s.getOrderById(orderId);
			long endTime = System.nanoTime();
			long duration = (endTime - startTime) / 1000; // Microseconds
			times[o] = duration;
		}
	}

	@Override
	public void run() {
		// Change this so we can specify what test it should launch.
		getOrdersByIdTest(test, options, warmup);
	}
}
