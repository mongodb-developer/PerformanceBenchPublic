package com.mongodb.devrel.pods.performancebench.models.apimonitor_lookup;

/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

import com.mongodb.client.*;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.devrel.pods.performancebench.SchemaTest;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.mongodb.client.model.Filters.*;

public class APIMonitorLookupTest implements SchemaTest {

    Logger logger;
    MongoClient mongoClient;
    MongoCollection<Document> apiCollection, metricsCollection;
    JSONObject args;
    JSONObject customArgs;

    //The following will be used to keep a note of the indexes that were on the collections
    //before we started. We'll drop these whilst testing, add the indexes our queries need
    //for the duration of the test runs, then drop those and re-instate the original indexes
    ArrayList<Document> metricsIndexes = new ArrayList<>();
    ArrayList<Document> apiIndexes = new ArrayList<>();

    public APIMonitorLookupTest() {
        logger = LoggerFactory.getLogger(APIMonitorLookupTest.class);
    }

    @Override
    public void cleanup(JSONObject args) {

        //Restore original indexes
        logger.info("Restoring indexes for: " + this.name());
        for (Document index : apiCollection.listIndexes()) {
            if(!(index.getString("name").equals("_id_"))){
                apiCollection.dropIndex(index.getString("name"));
            }
        }
        for (Document index : apiIndexes) {
            apiCollection.createIndex((Document)index.get("key"));
        }

        for (Document index : metricsCollection.listIndexes()) {
            if(!(index.getString("name").equals("_id_"))){
                metricsCollection.dropIndex(index.getString("name"));
            }
        }
        for (Document index : metricsIndexes) {
            metricsCollection.createIndex((Document)index.get("key"));
        }
        logger.info("Indexes restored");

        mongoClient.close();

    }

    @Override
    public void initialize(JSONObject args){

        //API: deployments.region_1
        //Metrics: apiDetails.appname_1_docType_1_creationDate_1

        this.args = args;

        //Retrieve settings for this particular model.
        this.customArgs = (JSONObject)args.get("custom");

        //Connect to the MongoDB instance with the test data. This can be a different instance
        //from the one PerformanceBench uses to save test results.
        mongoClient = MongoClients.create(customArgs.get("uri").toString());
        // Quick check of connection up front
        Document pingResult = mongoClient.getDatabase("system").runCommand(new Document("hello", 1));
        logger.debug(pingResult.toJson());

        apiCollection = mongoClient.getDatabase((String)customArgs.get("dbname")).getCollection((String)customArgs.get("apiCollectionName"));
        metricsCollection = mongoClient.getDatabase((String)customArgs.get("dbname")).getCollection((String)customArgs.get("metricsCollectionName"));

        //Are we rebuilding the data?
        if((Boolean)customArgs.get("rebuildData")){ rebuildData(); }

        //Drop any existing indexes on the collections and create the indexes required by the tests
        //(dropping existing indexes gives us a better chance of being able to get our indexes in to memory).
        logger.info("Creating indexes for: " + this.name());
        for (Document index : apiCollection.listIndexes()) {
            if(!(index.getString("name").equals("_id_"))){
                apiCollection.dropIndex(index.getString("name"));
                apiIndexes.add(index);
            }
        }
        apiCollection.createIndex(new Document("deployments.region", 1), new IndexOptions().sparse(false));

        for (Document index : metricsCollection.listIndexes()) {
            if(!(index.getString("name").equals("_id_"))){
                metricsCollection.dropIndex(index.getString("name"));
                metricsIndexes.add(index);
            }
        }
        metricsCollection.createIndex(new Document("apiDetails.appname", 1)
                .append("docType", 1)
                .append("creationDate", 1)
                , new IndexOptions().sparse(false));
        logger.info("Indexes created");

        /* Quick check things are working */
        logger.debug("Quick self test - just to see we are getting results");
        int apiID = 1;

        List<Document> d = getAPIById(apiID);
        logger.debug(name() + " Result size: " + d.size());
        if (d.isEmpty() || d.size() > 500) {
            logger.error("THIS DOESN'T LOOK CORRECT!!!");
            System.exit(1);
        }
    }

    @Override
    public String name() {
        return "APIMonitorLookupTest";
    }

    @Override
    public void warmup(JSONObject args) {
        // Collection scan will pull it all into cache if it can.
        // If it doesn't fit then we will part fill the cache
        apiCollection.find(new Document("not", "true")).first();
        metricsCollection.find(new Document("not", "true")).first();

    }

    public Document[] executeMeasure(int opsToTest, String measure, JSONObject args){

        //Branch based on the measure to be executed
        return switch (measure) {
            case "USEPIPELINE" -> getPipelineResults(opsToTest);
            default -> null;
        };
    }

    /**
     *
     * @param opsToTest - the number of iterations of the test to carry out
     * @return an array of BSON documents containing the execution time plus other
     *          metadata, for each test iteration. PerformanceBench will write these
     *          documents to a specified collection for later analysis.
     *
     * This method executes the API data aggregation process using the following
     * approach:
     *
     * 1. For each geographic region in turn, run an aggregation pipeline where the initial
     *      match stage retrieves all the API documents for API's in that region.
     * 2. After the initial match stage, perform a $lookup of corresponding metrics documents
     *      to determine the total number of calls to each api, as well as the success and
     *      failure rates of those calls, for the time period from the baseDate specified in
     *      the model's configuration file to the current date. The $lookup will use a sub-pipline
     *      to do these calculations.
     * 3. On completion of the aggregation pipeline, the returned metrics should be appended to the
     *      corresponding API documents retrieved by the query in step 1 (so the eventual output
     *      is comparable to the output of the $lookup pipeline).
     * 4. On completion of each test iteration, create a BSON document for each geographic region containing:
     *      The start time of the test iteration for this region (in epoch format)
     *      The duration of the test iteration for this region in milliseconds
     *      The name of the model (this.name())
     *      The name of the measure ("USEINQUERY")
     *      The geographic region tested
     *      The baseDate used to filter metrics records;
     *
     *      The total number of results documents returned should equal opsToTest * number of regions
     */
    private Document[] getPipelineResults(int opsToTest) {

        //We are going to run the aggregation once for each geographic region. Get the
        //list of regions from the model's configuration.
        JSONArray regions = (JSONArray)customArgs.get("regions");

        //We'll be filtering to only return data where the creationDate is
        //greater than or equal to "baseDate" in the model's configuration file.
        String baseDate = (String)customArgs.get("baseDate");

        String metricsCollectionName = (String)customArgs.get("metricsCollectionName");

        //The times array is used to store BSON documents with the execution
        //times of each iteration.
        Document[] times = new Document[opsToTest * regions.size()];
        int currentTest = 0;
        for (int o = 0; o < opsToTest; o++) {

            //Iterate for each region
            for(Object regionObj : regions) {

                String region = (String) regionObj;

                //Convert baseDate from the config file to a date object. We'll filter for records where the
                //creationDate is greater than or equal to this date.
                Instant instant = Instant.parse(baseDate); //"2000-01-01T00:00:00.000Z"
                Date baseTimeStamp = Date.from(instant);

                List<Document> aggPipeline = Arrays.asList(
                    new Document("$match", new Document("deployments.region", region)),
                    new Document("$lookup",
                        new Document("from", metricsCollectionName)
                        .append("let",
                            new Document("appName", "$apiDetails.appname")
                        )
                        .append("pipeline", Arrays.asList(
                            new Document("$match",
                                new Document("$expr",
                                    new Document("$and", Arrays.asList(
                                        new Document("$eq", Arrays.asList("$apiDetails.appname", "$$appName")),
                                        new Document("$gte", Arrays.asList("$creationDate", baseTimeStamp))
                                    ))
                                )
                            ),
                            new Document("$group",
                                new Document("_id", "$apiDetails.appname")
                                .append("totalVolume", new Document("$sum", "$transactionVolume"))
                                .append("totalError", new Document("$sum", "$errorCount"))
                                .append("totalSuccess", new Document("$sum", "$successCount"))
                            ),
                            new Document("$project",
                                new Document("aggregatedResponse",
                                    new Document("totalTransactionVolume", "$totalVolume")
                                    .append("errorRate",
                                        new Document("$cond", Arrays.asList(
                                            new Document("$eq", Arrays.asList("$totalVolume", 0L)),
                                            0L,
                                            new Document("$multiply", Arrays.asList(
                                                new Document("$divide", Arrays.asList("$totalError", "$totalVolume")),
                                                100L
                                            ))
                                        ))
                                    )
                                    .append("successRate",
                                        new Document("$cond", Arrays.asList(
                                            new Document("$eq", Arrays.asList("$totalVolume", 0L)),
                                            0L,
                                            new Document("$multiply", Arrays.asList(
                                                new Document("$divide", Arrays.asList("$totalSuccess", "$totalVolume")),
                                                100L
                                            ))
                                        ))
                                    )
                                )
                                .append("_id", 0L)
                            )
                        ))
                        .append("as", "results")
                    )
                );

                //Working in milliseconds. Use nanoTime if greater granularity required
                long startTime = System.currentTimeMillis();
                //long startTime = System.nanoTime();

                int metricsReturned = 0;
                long transactionVolume = 0L;
                int apisReturned = 0;
                MongoCursor<Document> cursor = apiCollection.aggregate(aggPipeline).iterator();
                try {
                    while (cursor.hasNext()) {
                        //The following is a check to see if metrics were found for each api
                        apisReturned++;
                        Document doc = (Document) cursor.next();
                        if(!(doc.getList("results", Document.class).isEmpty())){
                            metricsReturned++;
                            ArrayList<Document> results = (ArrayList<Document>)doc.get("results");
                            Document resultsDoc = (Document)results.get(0);
                            Document aggResults = (Document)resultsDoc.get("aggregatedResponse");
                            transactionVolume += (Integer)aggResults.get("totalTransactionVolume");
                        }
                    }
                } finally {
                    cursor.close();
                }

                long endTime = System.currentTimeMillis();
                long duration = (endTime - startTime);
                //Uncomment if using nanosecond granularity
                //long endTime = System.nanoTime();
                //long duration = (endTime - startTime) / 1000000; // Milliseconds
                Document results = new Document();
                results.put("startTime", startTime);
                results.put("endTime", endTime);
                results.put("duration", duration);
                results.put("model", this.name());
                results.put("measure", "USEPIPELINE");
                results.put("region", region);
                results.put("baseDate", baseTimeStamp);
                results.put("apiCount", apisReturned);
                results.put("metricsCount", metricsReturned);
                results.put("transactionsCount", transactionVolume);
                results.put("threads", ((Long)this.args.getOrDefault("threads", 2)).intValue());
                results.put("iterations", ((Long)this.args.getOrDefault("iterations", 2)).intValue());
                results.put("clusterTier", (String)this.customArgs.getOrDefault("clusterTier", "Not Specified"));

                times[currentTest] = results;
                currentTest++;
            }
        }
        return times;
    }

    private List<Document> getAPIById(int apiID) {
        List<Document> rval = new ArrayList<>();
        String apiIDString = String.format("api#%d", apiID);
        // Querying with _id field to avoid requiring index on a separate apiID field -

        Bson query = eq("_id", apiIDString);
        return apiCollection.find(query).into(rval);
    }

    private void rebuildData() {

        Integer apiCount = ((Long)customArgs.get("apiCount")).intValue();

        logger.info("Rebuilding test data for " + apiCount.toString() + " APIs");

        //Drop the existing collections if they exist:
        apiCollection.drop();
        metricsCollection.drop();
        //Recreate the handles to the collections - MongoDB will automatically recreate them
        //when we add documents
        apiCollection = mongoClient.getDatabase((String)customArgs.get("dbname")).getCollection((String)customArgs.get("apiCollectionName"));
        metricsCollection = mongoClient.getDatabase((String)customArgs.get("dbname")).getCollection((String)customArgs.get("metricsCollectionName"));

        //Arraylists for API documents and their corresponding metrics documents
        ArrayList<Document> apis = new ArrayList<>();
        ArrayList<Document> metrics = new ArrayList<>();

        //Get the list of regions
        JSONArray regions = (JSONArray)customArgs.get("regions");

        //Current date
        Instant currentDate = Instant.now();
        //Generate a random date within the last 90 days (this will be the date we use as the "API added" date
        Instant ninetyDaysAgo = currentDate.minus(Duration.ofDays(90));
        Instant apiDate = this.between(ninetyDaysAgo, currentDate);

        for (Integer i = 1; i <= apiCount; i++) {
            String apiname = "api#" + i.toString();

            //Randomly select a region for this API
            String region = (String)regions.get(ThreadLocalRandom.current().nextInt(0, regions.size()));

            //Build the document
            Document apiDoc = new Document("_id", apiname)
                    .append("apiDetails", new Document("appname", apiname)
                            .append("platform", "Linux")
                            .append("language", new Document("name", "Java")
                                    .append("version", "11.8.202")
                            )
                            .append("techStack", new Document("name", "Springboot")
                                    .append("version", "UNCATEGORIZED")
                            )
                            .append("environment", "PROD")
                    )
                    .append("deployments", new Document("region", region)
                            .append("createdAt", Date.from(apiDate))
                    );
            apis.add(apiDoc);

            //Generate a metric document for each 15 minute period from the apiDate until now.
            Integer metricNum = 1;
            Instant metricDate = apiDate;
            while(metricDate.isBefore(currentDate)){
                String metricName = apiname + "#S#" +metricNum.toString();
                metricNum++;
                //Create random transaction volumes / error rates / success rates
                Integer tv = ThreadLocalRandom.current().nextInt(0, 100000) + 1;
                Integer sc = ThreadLocalRandom.current().nextInt(0, tv) + 1;
                Integer ec = tv - sc;
                Document metricDoc = new Document("_id", metricName)
                        .append("apiDetails", new Document("appname", apiname))
                        .append("creationDate", Date.from(metricDate))
                        .append("transactionVolume", tv)
                        .append("errorCount", ec)
                        .append("successCount", sc)
                        .append("deployments", new Document("region", region));
                metrics.add(metricDoc);
                metricDate = metricDate.plus(Duration.ofMinutes(15));
            }
            if((apis.size() % 50 == 0)||(i == apiCount)){
                //Add API info in batches of 50 to keep arraylist size reasonable
                apiCollection.insertMany(apis);
                metricsCollection.insertMany(metrics);
                apis = new ArrayList<>();
                metrics = new ArrayList<>();
            }
        }
        logger.info("Test data rebuilt");
    }

    private Instant between(Instant startInclusive, Instant endExclusive) {

        long startSeconds = startInclusive.getEpochSecond();
        long endSeconds = endExclusive.getEpochSecond();
        long random = ThreadLocalRandom
                .current()
                .nextLong(startSeconds, endSeconds);
        return Instant.ofEpochSecond(random);
    }
}

