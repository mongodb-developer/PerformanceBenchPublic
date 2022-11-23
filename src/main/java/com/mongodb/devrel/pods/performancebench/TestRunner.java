/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mongodb.devrel.pods.performancebench;

/**
 *
 * @author graeme
 */
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.InsertManyResult;

import org.bson.Document;

//This class takes a List of test types and runs them
//NEEDS SPLIT IN TWO!
public class TestRunner implements Runnable {

    Logger logger;
    private JSONObject options;
    ExecutorService executorService;
    SchemaTest test;
    boolean warmup;

    int OPS_TO_TEST;
    Document[] times;
    private String subtest;

    /* Simulates N devices inserting X Documents */
    TestRunner() {
        logger = LoggerFactory.getLogger(TestRunner.class);
    }

    void prep(SchemaTest t, JSONObject o, boolean warmup, String st) {
        options = o;
        test = t;
        this.warmup = warmup;
        this.subtest = st;
        OPS_TO_TEST = ((Long)options.get("iterations")).intValue();
        times = new Document[OPS_TO_TEST];
    }

    void runTest(SchemaTest test, JSONObject testOpts) {

        try{
            options = testOpts;

            //Connect to MongoDB to store the results
            MongoClient mongoClient = MongoClients.create(options.get("resultsuri").toString());
            // Quick check of connection up front
            Document pingResult = mongoClient.getDatabase("system").runCommand(new Document("hello", 1));
            logger.debug(pingResult.toJson());

            MongoCollection resultsCollection = mongoClient.getDatabase((String)options.get("resultsDBName")).getCollection((String)options.get("resultsCollectionName"));

            final int NTHREADS = ((Long)options.getOrDefault("threads", 20)).intValue();
            /* Optimal here is actually a key tunable */
            OPS_TO_TEST = ((Long)options.get("iterations")).intValue();

            logger.debug("Warmup");
            test.warmup();

            JSONArray ja = (JSONArray)options.get("metrics");
            String[] subtests = new String[ja.size()];
            for(int i=0; i<ja.size(); i++) {
                subtests[i]=ja.get(i).toString();
            }

            logger.debug("Testing " + OPS_TO_TEST);

            for (String st : subtests) {
                executorService = Executors.newFixedThreadPool(NTHREADS);
                List<TestRunner> runners = new ArrayList<>();

                for (int t = 0; t < NTHREADS; t++) {
                    TestRunner tr = new TestRunner();
                    tr.prep(test, options, false, st);
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

                ArrayList<Document> alltimes = new ArrayList<>();
                for (TestRunner t : runners) {
                    if(t.times != null){
                        //Times may be null during testing.
                        for(Document d : t.times) {
                            alltimes.add(d);
                        }
                    }
                }

                InsertManyResult res = resultsCollection.insertMany(alltimes);
                logger.info(String.format(
                        "%s:%s %.1f result documents written",
                        test.name(), st, (double) res.getInsertedIds().size()));

//                Arrays.sort(alltimes);
//                int timelen = alltimes.length;
//                long total = 0;
//                for (int t = 0; t < timelen; t++) {
//                    total += alltimes[t];
//                }
//
//                double secs = (double) (endTime - startTime) / 1000000000.0;
//
//                logger.info(String.format(
//                        "%s:%s max(ms): %.2f, min(ms): %.2f, mean(ms): %.2f, 95th centile(ms): %.2f, throughput(qps): %.1f",
//                        test.name(), st,
//                        alltimes[timelen - 1] / 1000.0,
//                        alltimes[0] / 1000.0,
//                        (total / 1000.0) / timelen,
//                        alltimes[(int) (timelen * .95)] / 1000.0,
//                        (double) timelen / secs));

            }
        }
        finally{
            test.cleanup();
        }
    }

    @Override
    public void run() {
        times = test.executeMeasure(OPS_TO_TEST, subtest, options, warmup);
    }

}
