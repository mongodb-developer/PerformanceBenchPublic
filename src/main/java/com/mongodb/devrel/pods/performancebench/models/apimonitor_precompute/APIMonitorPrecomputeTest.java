package com.mongodb.devrel.pods.performancebench.models.apimonitor_precompute;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.TimeSeriesGranularity;
import com.mongodb.client.model.TimeSeriesOptions;
import com.mongodb.devrel.pods.performancebench.SchemaTest;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.mongodb.client.model.Filters.eq;



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

public class APIMonitorPrecomputeTest implements SchemaTest {

    Logger logger;
    MongoClient mongoClient;
    MongoCollection<Document> apiCollection, metricsCollection, precomputeCollection;

    //Reactive Streams Driver versions:
    com.mongodb.reactivestreams.client.MongoClient reactiveClient;
    com.mongodb.reactivestreams.client.MongoCollection<Document> reactiveAPICollection, reactivePrecomputeCollection;

    JSONObject args;
    JSONObject customArgs;

    //The following will be used to keep a note of the indexes that were on the collections
    //before we started. We'll drop these whilst testing, add the indexes our queries need
    //for the duration of the test runs, then drop those and re-instate the original indexes
    ArrayList<Document> metricsIndexes = new ArrayList<>();
    ArrayList<Document> apiIndexes = new ArrayList<>();
    ArrayList<Document> precomputeIndexes = new ArrayList<>();

    public APIMonitorPrecomputeTest() {
        logger = LoggerFactory.getLogger(com.mongodb.devrel.pods.performancebench.models.apimonitor_precompute.APIMonitorPrecomputeTest.class);
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
        for (Document index : precomputeCollection.listIndexes()) {
            if(!(index.getString("name").equals("_id_"))){
                precomputeCollection.dropIndex(index.getString("name"));
            }
        }
        for (Document index : precomputeIndexes) {
            precomputeCollection.createIndex((Document)index.get("key"));
        }
        logger.info("Indexes restored");


        mongoClient.close();
        reactiveClient.close();

    }

    @Override
    public void initialize(JSONObject args){

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
        precomputeCollection = mongoClient.getDatabase((String)customArgs.get("dbname")).getCollection((String)customArgs.get("precomputeCollectionName"));

        //Are we rebuilding the data?
        if((Boolean)customArgs.get("rebuildData")){ rebuildData(); }

        //Connect with the Reactive Streams Driver
        reactiveClient = com.mongodb.reactivestreams.client.MongoClients.create(customArgs.get("uri").toString());
        SubscriberHelpers.OperationSubscriber<Document> subscriber = new SubscriberHelpers.OperationSubscriber<Document>(){
            @Override
            public void onNext(final Document result) {
                logger.debug(result.toJson());
            }
        };
        reactiveClient.getDatabase("system").runCommand(new Document("hello", 1)).subscribe(subscriber);
        subscriber.await();
        reactiveAPICollection = reactiveClient.getDatabase((String)customArgs.get("dbname")).getCollection((String)customArgs.get("apiCollectionName"));
        reactivePrecomputeCollection = reactiveClient.getDatabase((String)customArgs.get("dbname")).getCollection((String)customArgs.get("precomputeCollectionName"));

        //Drop any existing indexes on the collections and create the indexes required by the tests
        //(dropping existing indexes gives us a better chance of being able to get our indexes in to memory).
        logger.info("Creating indexes for: " + this.name());
        for (Document index : apiCollection.listIndexes()) {
            if(!(index.getString("name").equals("_id_"))){
                apiCollection.dropIndex(index.getString("name"));
                apiIndexes.add(index);
            }
        }

        for (Document index : metricsCollection.listIndexes()) {
            if(!(index.getString("name").equals("_id_"))){
                metricsCollection.dropIndex(index.getString("name"));
                metricsIndexes.add(index);
            }
        }

        for (Document index : precomputeCollection.listIndexes()) {
            if(!(index.getString("name").equals("_id_"))){
                precomputeCollection.dropIndex(index.getString("name"));
                precomputeIndexes.add(index);
            }
        }

        apiCollection.createIndex(new Document("deployments.region", 1), new IndexOptions().sparse(false));
        metricsCollection.createIndex(new Document("region", 1)
                .append("year", 1)
                .append("dayOfYear", 1)
                .append("creationDate", 1), new IndexOptions().sparse(false));
        precomputeCollection.createIndex(new Document("region", 1)
                .append("dateTag", 1), new IndexOptions().sparse(false));

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
        return "APIMonitorPrecomputeTest";
    }

    @Override
    public void warmup(JSONObject args) {
        // Collection scan will pull all data in the collection into cache if it can.
        // If it doesn't fit then we will part fill the cache;
        apiCollection.find(new Document("not", "true")).first();
        metricsCollection.find(new Document("not", "true")).first();
        precomputeCollection.find(new Document("not", "true")).first();
    }

    @Override
    public Document[] executeMeasure(int opsToTest, String measure, JSONObject args){

        //Branch based on the measure to be executed
        return switch (measure) {
            case "PRECOMPUTE" -> getPrecomputeResults(opsToTest);
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
     * approach with the MongoDB Reactive Streams driver:
     *
     * 1. For each geographic region in turn, run a query to retrieve all the API
     *      documents for the APIs in that region from the primary API Details collection.
     * 2. *Simultaneously*, run an aggregation pipeline to determine the total number of calls to each
     *      api, as well as the success and failure rates of those calls, for the time period from the
     *      baseDate specified in the model's configuration file to the current date.
     *      This query will use pre-computed totals for each API for years, months and days-of month and
     *      only use the raw 15-minute values for the partial days at either end of the selected time
     *      period. This should result in a much smaller number of documents that the pipeline needs
     *      to work with, and we are assessing whether that smaller number makes a noticeable difference
     *      in performance.
     * 3. The initial match stage in the pipeline described is step 2 should filter on the metrics
     *      documents' deployment.region field (rather than do a $in expression on apiDetails.appname),
     *      to identify the relevant metrics documents.
     * 4. On completion of the aggregation pipeline, the returned metrics should be appended to the
     *      corresponding API documents retrieved by the query in step 1 (so the eventual output
     *      is comparable to the output of the $lookup pipeline).
     * 5. On completion of each test iteration, create a BSON document for each geographic region containing:
     *      The start time of the test iteration for this region (in epoch format)
     *      The duration of the test iteration for this region in milliseconds
     *      The name of the model (this.name())
     *      The name of the measure ("PRECOMPUTE")
     *      The geographic region tested
     *      The baseDate used to filter metrics records;
     *
     *      The total number of results documents returned should equal opsToTest * number of regions
     */
    private Document[] getPrecomputeResults(int opsToTest) {


        //We are going to run the aggregation once for each geographic region. Get the
        //list of regions from the model's configuration.
        JSONArray regions = (JSONArray)customArgs.get("regions");

        //We'll be filtering to only return data where the creationDate is
        //greater than or equal to "baseDate" in the model's configuration file.
        String baseDate = (String)customArgs.get("baseDate");

        //The times array is used to store BSON documents with the execution
        //times of each iteration.
        Document[] times = new Document[opsToTest * regions.size()];
        int currentTest = 0;

        //Convert baseDate from the config file to a date object. We'll filter for records where the
        //creationDate is greater than or equal to this date.
        Instant baseInstant = Instant.parse(baseDate); //Expected format is "2000-01-01T00:00:00.000Z"
        Date baseTimeStamp = Date.from(baseInstant);

        //Need to do some calculations here on the dates
        //Our start date for the range of dates we want to summarize data is the basedate - get the
        //integer year, month, and day of month values from it.
        Integer baseYear = baseInstant.atZone(ZoneOffset.UTC).getYear();
        Integer baseMonth = baseInstant.atZone(ZoneOffset.UTC).getMonthValue();
        Integer baseDOM = baseInstant.atZone(ZoneOffset.UTC).getDayOfMonth();
        Integer baseDOY = baseInstant.atZone(ZoneOffset.UTC).getDayOfYear();

        //End date is the current date
        Instant nowInstant = Instant.now();
        Date nowTimeStamp = Date.from(nowInstant);
        Integer nowYear = nowInstant.atZone(ZoneOffset.UTC).getYear();
        Integer nowMonth = nowInstant.atZone(ZoneOffset.UTC).getMonthValue();
        Integer nowDOM = nowInstant.atZone(ZoneOffset.UTC).getDayOfMonth();
        Integer nowDOY = nowInstant.atZone(ZoneOffset.UTC).getDayOfYear();

        //Calculate the list of precomputed values we want to retrieve - these are complete days, months or years
        //between the base date and current date.
        ArrayList<String> precomputes = new ArrayList<>();

        //Start with complete years
        for(Integer i = baseYear + 1; i < nowYear; i++){
            precomputes.add(i.toString());
        }

        //Now months - this should be:
        // the month following the base date month up to the "now" month exclusive if they occur in the same year, OR
        // the months following the base date month up to the end of that year, plus January up to the "now"
        //  month (exclusive) in the year in which it occurs, if the base date and now date are in different years
        if(!baseYear.equals(nowYear)){
            for(Integer i = baseMonth + 1; i <= 12; i++){
                precomputes.add(baseYear.toString() + "-" + i.toString());
            }
            for(Integer i = 1; i < nowMonth; i++){
                precomputes.add(nowYear.toString() + "-" + i.toString());
            }
        }else{
            for(Integer i = baseMonth + 1; i < nowMonth; i++){
                precomputes.add(baseYear.toString() + "-" + i.toString());
            }
        }

        //Now days - this should be:
        // the day following the base DOM up to the "now" DOM exclusive if they occur in the same year and month, OR
        // the days following the base DOM up to the end of that month, plus the first up to the "now"
        //  DOM in the month in which it occurs (exclusive), if the base date and now date are in different years

        //Note - we'll have entries for the 29th, 30th and 31st even in months that don't have those days. The
        //query will just generate no hits for those days. Keeps the following clauses cleaner.

        if((!baseYear.equals(nowYear))||(!baseMonth.equals(nowMonth))){
            for(Integer i = baseDOM + 1; i <= 31; i++){
                precomputes.add(baseYear.toString() + "-" + baseMonth.toString() + "-" + i.toString());
            }
            for(Integer i = 1; i < nowDOM; i++){
                precomputes.add(nowYear.toString() + "-" + nowMonth.toString() + "-" + i.toString());
            }
        }else{
            for(Integer i = baseDOM + 1; i < nowDOM; i++){
                precomputes.add(baseYear.toString() + "-" + baseMonth.toString() + "-" + i.toString());
            }
        }



        for (int o = 0; o < opsToTest; o++) {

            //Iterate for each region
            for(Object regionObj : regions) {

                String region = (String) regionObj;

                //Get the APIs for the selected region
                ArrayList<Document> apis = new ArrayList<>();

                Bson filter = eq("deployments.region", region);
                SubscriberHelpers.OperationSubscriber<Document> apiSubscriber = new SubscriberHelpers.OperationSubscriber<Document>(){
                    @Override
                    public void onNext(final Document api) {
                        apis.add(api);
                    }
                };


                //Simultaneously, for each API, get the metrics using a pipeline that pulls 15 minute metrics records
                //for the partial days at each end of the specified time window, plus the precomputed values for each
                //complete day, month or year between the days at each end of the specified time window.
                //initial match
                ArrayList<Document> results = new ArrayList<>();
                List<Document> aggPipeline = Arrays.asList(new Document("$match",
                    new Document("region", region)
                                .append("dateTag", new Document("$in", precomputes))),
                        new Document("$unionWith",
                                new Document("coll", (String)customArgs.get("metricsCollectionName"))
                                        .append("pipeline", Arrays.asList(new Document("$match",
                                                new Document("$expr",
                                                        new Document("$or", Arrays.asList(new Document("$and", Arrays.asList(new Document("$eq", Arrays.asList("$region", region)),
                                                                        new Document("$eq", Arrays.asList("$year", baseYear)),
                                                                        new Document("$eq", Arrays.asList("$dayOfYear", baseDOY)),
                                                                        new Document("$gte", Arrays.asList("$creationDate", baseTimeStamp)))),
                                                                new Document("$and", Arrays.asList(new Document("$eq", Arrays.asList("$region", region)),
                                                                        new Document("$eq", Arrays.asList("$year", nowYear)),
                                                                        new Document("$eq", Arrays.asList("$dayOfYear", nowDOY)),
                                                                        new Document("$lte", Arrays.asList("$creationDate", nowTimeStamp))))))))))),
                        new Document("$group",
                                new Document("_id", "$appname")
                                        .append("totalVolume",
                                                new Document("$sum", "$transactionVolume"))
                                        .append("totalError",
                                                new Document("$sum", "$errorCount"))
                                        .append("totalSuccess",
                                                new Document("$sum", "$successCount"))
                                        .append("region",
                                                new Document("$first", "$region"))),
                        new Document("$project",
                                new Document("aggregatedresponse",
                                        new Document("totalTransactionVolume", "$totalVolume")
                                                .append("errorRate",
                                                        new Document("$cond", Arrays.asList(new Document("$eq", Arrays.asList("$totalVolume", 0L)), 0L,
                                                                new Document("$multiply", Arrays.asList(new Document("$divide", Arrays.asList("$totalError", "$totalVolume")), 100L)))))
                                                .append("successRate",
                                                        new Document("$cond", Arrays.asList(new Document("$eq", Arrays.asList("$totalVolume", 0L)), 0L,
                                                                new Document("$multiply", Arrays.asList(new Document("$divide", Arrays.asList("$totalSuccess", "$totalVolume")), 100L))))))));

                SubscriberHelpers.OperationSubscriber<Document> metricsSubscriber = new SubscriberHelpers.OperationSubscriber<Document>(){
                    @Override
                    public void onNext(final Document result) {
                        results.add(result);
                    }
                };

                //Working in milliseconds. Use nanoTime if greater granularity required
                long startTime = System.currentTimeMillis();
                //long startTime = System.nanoTime();

                reactiveAPICollection.find(filter).subscribe(apiSubscriber);
                reactivePrecomputeCollection.aggregate(aggPipeline).subscribe(metricsSubscriber);
                apiSubscriber.await();
                metricsSubscriber.await();
                //Merge the results into the API documents
                int metricsReturned = 0;
                long transactionVolume = 0L;
                for (int i = 0; i < apis.size(); i++) {
                    Document api = apis.get(i);
                    for (int y = 0; y < results.size(); y++) {
                        Document result = results.get(y);
                        if (((String) api.get("_id")).equals((String) result.get("_id"))) {
                            metricsReturned++;
                            transactionVolume += (Integer)((Document)result.get("aggregatedresponse")).get("totalTransactionVolume");
                            api.append("results", result);
                            break;
                        }
                    }
                }

                long endTime = System.currentTimeMillis();
                long duration = (endTime - startTime);
                //Uncomment if using nanosecond granularity
                //long endTime = System.nanoTime();
                //long duration = (endTime - startTime) / 1000000; // Milliseconds
                Document metrics = new Document();
                metrics.put("startTime", startTime);
                metrics.put("endTime", endTime);
                metrics.put("duration", duration);
                metrics.put("model", this.name());
                metrics.put("measure", "PRECOMPUTE");
                metrics.put("region", region);
                metrics.put("baseDate", baseTimeStamp);
                metrics.put("apiCount", apis.size());
                metrics.put("metricsCount", metricsReturned);
                metrics.put("transactionsCount", transactionVolume);
                metrics.put("threads", ((Long)this.args.getOrDefault("threads", 2)).intValue());
                metrics.put("iterations", ((Long)this.args.getOrDefault("iterations", 2)).intValue());
                metrics.put("clusterTier", (String)this.customArgs.getOrDefault("clusterTier", "Not Specified"));
                times[currentTest] = metrics;
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

        MongoCollection<Document> apiCollection, metricsCollection, precomputeCollection, tsMetricsCollection;
        apiCollection = mongoClient.getDatabase((String)customArgs.get("dbname")).getCollection((String)customArgs.get("apiCollectionName"));
        metricsCollection = mongoClient.getDatabase((String)customArgs.get("dbname")).getCollection((String)customArgs.get("metricsCollectionName"));
        tsMetricsCollection = mongoClient.getDatabase((String)customArgs.get("dbname")).getCollection((String)customArgs.get("tsMetricsCollectionName"));
        precomputeCollection = mongoClient.getDatabase((String)customArgs.get("dbname")).getCollection((String)customArgs.get("precomputeCollectionName"));

        //Drop the existing collections if they exist:
        apiCollection.drop();
        metricsCollection.drop();
        tsMetricsCollection.drop();
        precomputeCollection.drop();
        //Recreate the handles to the collections - MongoDB will automatically recreate them
        //when we add documents
        apiCollection = mongoClient.getDatabase((String)customArgs.get("dbname")).getCollection((String)customArgs.get("apiCollectionName"));
        metricsCollection = mongoClient.getDatabase((String)customArgs.get("dbname")).getCollection((String)customArgs.get("metricsCollectionName"));
        precomputeCollection = mongoClient.getDatabase((String)customArgs.get("dbname")).getCollection((String)customArgs.get("precomputeCollectionName"));

        //The time-series collection needs to be explicitly created
        TimeSeriesOptions tsOptions = new TimeSeriesOptions("creationDate");
        tsOptions = tsOptions.metaField("region");
        tsOptions = tsOptions.granularity(TimeSeriesGranularity.HOURS);
        CreateCollectionOptions collOptions = new CreateCollectionOptions().timeSeriesOptions(tsOptions);
        mongoClient.getDatabase((String)customArgs.get("dbname")).createCollection((String)customArgs.get("tsMetricsCollectionName"), collOptions);
        tsMetricsCollection = mongoClient.getDatabase((String)customArgs.get("dbname")).getCollection((String)customArgs.get("tsMetricsCollectionName"));


        //Arraylists for API documents and their corresponding metrics documents
        ArrayList<Document> apis = new ArrayList<>();
        ArrayList<Document> metrics = new ArrayList<>();

        //Get the list of regions
        JSONArray regions = (JSONArray)customArgs.get("regions");

        //Current date
        Instant currentDate = Instant.now();
        Instant ninetyDaysAgo = currentDate.minus(Duration.ofDays(90));


        for (Integer i = 1; i <= apiCount; i++) {
            String apiname = "api#" + i.toString();

            //Generate a random date within the last 90 days (this will be the date we use as the "API added" date
            Instant apiDate = this.between(ninetyDaysAgo, currentDate);

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
                        .append("appname", apiname)
                        .append("creationDate", Date.from(metricDate))
                        .append("transactionVolume", tv)
                        .append("errorCount", ec)
                        .append("successCount", sc)
                        .append("region", region)
                        .append("year", metricDate.atZone(ZoneOffset.UTC).getYear())
                        .append("monthOfYear", metricDate.atZone(ZoneOffset.UTC).getMonthValue())
                        .append("dayOfMonth", metricDate.atZone(ZoneOffset.UTC).getDayOfMonth())
                        .append("dayOfYear", metricDate.atZone(ZoneOffset.UTC).getDayOfYear());
                metrics.add(metricDoc);
                metricDate = metricDate.plus(Duration.ofMinutes(15));
            }
            if((apis.size() % 50 == 0)||(i == apiCount)){
                //Add API info in batches of 50 to keep arraylist size reasonable
                apiCollection.insertMany(apis);
                metricsCollection.insertMany(metrics);
                tsMetricsCollection.insertMany(metrics);
                apis = new ArrayList<>();
                metrics = new ArrayList<>();
            }
        }

        //Rebuild precomputedata

        //Start with years
        List<Document> precalcPipeline = Arrays.asList(new Document("$group",
                        new Document("_id",
                                new Document("$concat", Arrays.asList("$appname", "#Y#",
                                        new Document("$toString", "$year"))))
                                .append("transactionVolume",
                                        new Document("$sum", "$transactionVolume"))
                                .append("errorCount",
                                        new Document("$sum", "$errorCount"))
                                .append("successCount",
                                        new Document("$sum", "$successCount"))
                                .append("region",
                                        new Document("$first", "$region"))
                                .append("appname",
                                        new Document("$first", "$appname"))
                                .append("metricsCount",
                                        new Document("$sum", 1L))
                                .append("year",
                                        new Document("$first", "$year"))),
                new Document("$set",
                        new Document("type", "year_precalc")
                                .append("dateTag",
                                        new Document("$toString", "$year"))),
                new Document("$merge",
                        new Document("into", (String)customArgs.get("precomputeCollectionName"))
                                .append("on", "_id")
                                .append("whenMatched", "replace")));
        MongoCursor<Document> cursor = metricsCollection.aggregate(precalcPipeline).iterator();
        cursor.close();


        //Next months
        precalcPipeline = Arrays.asList(new Document("$group",
                        new Document("_id",
                                new Document("$concat", Arrays.asList("$appname", "#Y#",
                                        new Document("$toString", "$year"), "#M#",
                                        new Document("$toString", "$monthOfYear"))))
                                .append("transactionVolume",
                                        new Document("$sum", "$transactionVolume"))
                                .append("errorCount",
                                        new Document("$sum", "$errorCount"))
                                .append("successCount",
                                        new Document("$sum", "$successCount"))
                                .append("region",
                                        new Document("$first", "$region"))
                                .append("appname",
                                        new Document("$first", "$appname"))
                                .append("metricsCount",
                                        new Document("$sum", 1L))
                                .append("year",
                                        new Document("$first", "$year"))
                                .append("monthOfYear",
                                        new Document("$first", "$monthOfYear"))),
                new Document("$set",
                        new Document("type", "month_precalc")
                                .append("dateTag",
                                        new Document("$concat", Arrays.asList(new Document("$toString", "$year"), "-",
                                                new Document("$toString", "$monthOfYear"))))),
                new Document("$merge",
                        new Document("into", (String)customArgs.get("precomputeCollectionName"))
                                .append("on", "_id")
                                .append("whenMatched", "replace")));
        cursor = metricsCollection.aggregate(precalcPipeline).iterator();
        cursor.close();

        //Finally, days of month
        precalcPipeline = Arrays.asList(new Document("$group",
                        new Document("_id",
                                new Document("$concat", Arrays.asList("$appname", "#Y#",
                                        new Document("$toString", "$year"), "#M#",
                                        new Document("$toString", "$monthOfYear"), "#D#",
                                        new Document("$toString", "$dayOfMonth"))))
                                .append("transactionVolume",
                                        new Document("$sum", "$transactionVolume"))
                                .append("errorCount",
                                        new Document("$sum", "$errorCount"))
                                .append("successCount",
                                        new Document("$sum", "$successCount"))
                                .append("region",
                                        new Document("$first", "$region"))
                                .append("appname",
                                        new Document("$first", "$appname"))
                                .append("metricsCount",
                                        new Document("$sum", 1L))
                                .append("year",
                                        new Document("$first", "$year"))
                                .append("monthOfYear",
                                        new Document("$first", "$monthOfYear"))
                                .append("dayOfMonth",
                                        new Document("$first", "$dayOfMonth"))),
                new Document("$set",
                        new Document("type", "dom_precalc")
                                .append("dateTag",
                                        new Document("$concat", Arrays.asList(new Document("$toString", "$year"), "-",
                                                new Document("$toString", "$monthOfYear"), "-",
                                                new Document("$toString", "$dayOfMonth"))))),
                new Document("$merge",
                        new Document("into", (String)customArgs.get("precomputeCollectionName"))
                                .append("on", "_id")
                                .append("whenMatched", "replace")));
        cursor = metricsCollection.aggregate(precalcPipeline).iterator();
        cursor.close();

        //Reset the class level collection references:
        this.apiCollection = apiCollection;
        this.metricsCollection = metricsCollection;
        this.precomputeCollection = precomputeCollection;

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

