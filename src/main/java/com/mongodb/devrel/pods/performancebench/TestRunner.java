/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mongodb.devrel.pods.performancebench;

/**
 *
 * @author graeme
 */

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.InsertManyResult;
import org.bson.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of the Runnable interface that executes each of the measures defined for a model class a specified number
 * of times in a specified number of concurrent threads (to mimic a multi-user / device environment),and then writes
 * documents containing the results of each test execution to a MongoDB collection.
 *
 * The model classes used to execute the measures are dynamic, and can carry out any type of test required by a
 * given scenario / use-case, as long as they implement the SchemaTest interface.
 *
 */
public class TestRunner implements Runnable {

    Logger logger;

    //Application configuration options - see the README for details of expected format.
    private JSONObject options;

    //Used to execute the specified measure asynchronously in a specified number of threads (to allow modelling
    //of a multi-user environment
    ExecutorService executorService;

    //An instance of the test class that implements the measure functionality to be executed
    SchemaTest test;

    //The number of iterations of the measure to be executed by each thread.
    int OPS_TO_TEST;

    //An array of BSON documents containing the results of each measure iteration. These documents will be written to
    //a MongoDB collection specified in the application JSON config file. The contents of each document is up the
    //test class implementor.
    Document[] times;

    //The specific measure to be executed.
    private String subtest;


    TestRunner(SchemaTest t, JSONObject o, String st) {

        logger = LoggerFactory.getLogger(TestRunner.class);
        options = o;
        test = t;
        this.subtest = st;
        OPS_TO_TEST = ((Long)options.get("iterations")).intValue();
        times = new Document[OPS_TO_TEST];

    }

    TestRunner() {
        logger = LoggerFactory.getLogger(TestRunner.class);
    }

    void runTest(SchemaTest test, JSONObject testOpts) {

        options = testOpts;

        //Connect to MongoDB instance specified for store test execution results (defaults to localhost:27017)
        MongoClient mongoClient = MongoClients.create(options.getOrDefault("resultsuri", "mongodb://localhost:27017:").toString());
        // Quick check of connection up front
        Document pingResult = mongoClient.getDatabase("system").runCommand(new Document("hello", 1));
        logger.debug(pingResult.toJson());

        //Get the specified collection into which results documents should be written (default: PerformanceBench.Results)
        MongoCollection resultsCollection = mongoClient.getDatabase((String)options.getOrDefault("resultsDBName", "PerformanceBench"))
                .getCollection((String)options.getOrDefault("resultsCollectionName", "Results"));

        //The following specifies the number of concurrent threads in which measures will be executed (default: 2)
        final int NTHREADS = ((Long)options.getOrDefault("threads", 2)).intValue();

        //The following specifies the number of iterations of each measure executed by each thread (default: 5).
        //The total number of executions of each measure is thus NTHREADS * OPS_TO_TEST
        OPS_TO_TEST = ((Long)options.getOrDefault("iterations", 5)).intValue();

        //Execute the model class' warmup method. This is intended to set the system in a realistic state by -
        //for example - preloading data into cache.
        test.warmup(testOpts);

        JSONArray ja = (JSONArray)options.get("measures");
        String[] measures = new String[ja.size()];
        for(int i=0; i<ja.size(); i++) {
            measures[i]=ja.get(i).toString();
        }

        for (String mr : measures) {
            //Loop once for each measure defined for the model.
            executorService = Executors.newFixedThreadPool(NTHREADS);
            List<TestRunner> runners = new ArrayList<>();

            for (int t = 0; t < NTHREADS; t++) {
                //Instantiate a further Testrunner object for each concurrent execution thread
                //and then initiate execution of the measures.
                TestRunner tr = new TestRunner(test, options, mr);
                runners.add(tr);
                executorService.execute(tr); // calls run()
            }
            long startTime = System.nanoTime();

            //Wait for each thread to complete execution of the specified number of iterations of each measure.
            executorService.shutdown();
            try {
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
            }

            long endTime = System.nanoTime();

            //Create a consolidated list of result BSON documents from each thread
            ArrayList<Document> alltimes = new ArrayList<>();
            for (TestRunner t : runners) {
                if(t.times != null){
                    //Times may be null during testing.
                    for(Document d : t.times) {
                        alltimes.add(d);
                    }
                }
            }
            //Insert the result documents into the specified MongoDB collection
            InsertManyResult res = resultsCollection.insertMany(alltimes);
            logger.info(String.format(
                "%s:%s %.1f result documents written",
                test.name(), mr, (double) res.getInsertedIds().size()));

        }

    }

    @Override
    public void run() {
        times = test.executeMeasure(OPS_TO_TEST, subtest, options);
    }

}
