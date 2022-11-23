package com.mongodb.devrel.pods.performancebench.models.apimonitor_lookup;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.devrel.pods.performancebench.SchemaTest;
import com.mongodb.devrel.pods.performancebench.utilities.RecordFactory;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    MongoCollection<Document> apiCollection, metricsCollection, singleCollection;
    RecordFactory recordFactory = new RecordFactory();
    JSONObject args;
    JSONObject customArgs;

    public APIMonitorLookupTest() {
        logger = LoggerFactory.getLogger(APIMonitorLookupTest.class);
    }

    @Override
    public void cleanup() {
        mongoClient.close();
    }

    @Override
    public void initialize(JSONObject newArgs){

        this.args = newArgs;
        this.customArgs = (JSONObject)args.get("custom");

        mongoClient = MongoClients.create(customArgs.get("uri").toString());
        // Quick check of connection up front
        Document pingResult = mongoClient.getDatabase("system").runCommand(new Document("hello", 1));
        logger.debug(pingResult.toJson());

        apiCollection = mongoClient.getDatabase((String)customArgs.get("dbname")).getCollection((String)customArgs.get("apiCollectionName"));
        metricsCollection = mongoClient.getDatabase((String)customArgs.get("dbname")).getCollection((String)customArgs.get("metricsCollectionName"));
        singleCollection = mongoClient.getDatabase((String)customArgs.get("dbname")).getCollection((String)customArgs.get("singleCollectionName"));

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
    public void warmup() {
        apiCollection.find(new Document("not", "true")).first(); // Collection scan will pull it all into cache if it can
        metricsCollection.find(new Document("not", "true")).first(); // Collection scan will pull it all into cache if it can
        singleCollection.find(new Document("not", "true")).first(); // Collection scan will pull it all into cache if it can

        // If it doesn't fit then we will part fill the cache;

    }

    @Override
    public Document[] executeMeasure(int opsToTest, String subtest, JSONObject args, boolean warmup){

        return switch (subtest) {
            case "USEPIPELINE" -> getPipelineResults(opsToTest, args, warmup);
            case "USEMULTIQUERY" -> getMultiQueryResults(opsToTest, args, warmup);
            case "USESINGLECOLLECTION" -> getSingleCollectionResults(opsToTest, args, warmup);
            default -> null;
        };
    }

    private Document[] getPipelineResults(int opsToTest, JSONObject testOptions, boolean warmup) {

        JSONArray regions = (JSONArray)((JSONObject)testOptions.get("custom")).get("regions");
        String baseDate = (String)((JSONObject)testOptions.get("custom")).get("baseDate");
        String metricsCollectionName = (String)customArgs.get("metricsCollectionName");

        Document[] times = new Document[opsToTest * regions.size()];
        int currentTest = 0;
        for (int o = 0; o < opsToTest; o++) {

            ////Select a random region
            //String region = (String)regions.get(ThreadLocalRandom.current().nextInt(regions.size()));

            for(Object regionObj : regions) {

                String region = (String) regionObj;

                Instant instant = Instant.parse(baseDate); //"2000-01-01T00:00:00.000Z"
                Date baseTimeStamp = Date.from(instant);

                List<Document> aggPipeline = Arrays.asList(
                        new Document("$match", new Document("deployments.region", region)),
                        new Document("$lookup",
                                new Document("from", metricsCollectionName)
                                        .append("let",
                                                new Document("appName", "$apiDetails.appname"))
                                        .append("pipeline", Arrays.asList(new Document("$match",
                                                        new Document("$expr",
                                                                new Document("$and", Arrays.asList(new Document("$eq", Arrays.asList("$apiDetails.appname", "$$appName")),
                                                                        new Document("$and", Arrays.asList(new Document("$gte", Arrays.asList("$creationDate", baseTimeStamp)),
                                                                                new Document("$lte", Arrays.asList("$creationDate", "$$NOW")))))))),
                                                //                                                                    new Document("$and", Arrays.asList(new Document("$gte", Arrays.asList("$creationDate", "$$date")),
                                                //                                                                            new Document("$lte", Arrays.asList("$creationDate", "$$NOW")))))))),
                                                new Document("$group",
                                                        new Document("_id", "$apiDetails.appname")
                                                                .append("totalVolume",
                                                                        new Document("$sum", "$transactionVolume"))
                                                                .append("totalError",
                                                                        new Document("$sum", "$errorCount"))
                                                                .append("totalSuccess",
                                                                        new Document("$sum", "$successCount"))),
                                                new Document("$project",
                                                        new Document("aggregatedResponse",
                                                                new Document("totalTransactionVolume", "$totalVolume")
                                                                        .append("errorRate",
                                                                                new Document("$cond", Arrays.asList(new Document("$eq", Arrays.asList("$totalVolume", 0L)), 0L,
                                                                                        new Document("$multiply", Arrays.asList(new Document("$divide", Arrays.asList("$totalError", "$totalVolume")), 100L)))))
                                                                        .append("successRate",
                                                                                new Document("$cond", Arrays.asList(new Document("$eq", Arrays.asList("$totalVolume", 0L)), 0L,
                                                                                        new Document("$multiply", Arrays.asList(new Document("$divide", Arrays.asList("$totalSuccess", "$totalVolume")), 100L))))))
                                                                .append("_id", 0L))))
                                        .append("as", "results")),
                        new Document("$sort",
                                new Document("results.aggregatedResponse.totalTransactionVolume", -1L)
                                        .append("apiDetails.appName", 1L)),
                        new Document("$limit", 1000L));


                long epTime = System.currentTimeMillis(); //nanotime is NOT necessarily epoch time so don't use it as a timestamp
                long startTime = System.nanoTime();
                try {
                    apiCollection.aggregate(aggPipeline).forEach(doc -> System.out.println(doc.toJson()));
                } catch (Exception e) {
                }
                long endTime = System.nanoTime();
                long duration = (endTime - startTime) / 1000000; // Milliseconds
                Document results = new Document();
                results.put("startTime", epTime);
                results.put("duration", duration);
                results.put("test", this.name());
                results.put("subtest", "USEPIPELINE");
                results.put("region", region);
                results.put("startDate", baseTimeStamp);
                times[currentTest] = results;
                currentTest++;
            }
        }
        return times;
    }

    private Document[] getMultiQueryResults(int opsToTest, JSONObject testOptions, boolean warmup) {

        JSONArray regions = (JSONArray)((JSONObject)testOptions.get("custom")).get("regions");
        String baseDate = (String)((JSONObject)testOptions.get("custom")).get("baseDate");
        String metricsCollectionName = (String)customArgs.get("metricsCollectionName");

        Document[] times = new Document[opsToTest * regions.size()];
        int currentTest = 0;

        for (int o = 0; o < opsToTest; o++) {

            ////Select a random region
            //String region = (String)regions.get(ThreadLocalRandom.current().nextInt(regions.size()));
            for(Object regionObj : regions) {

                String region = (String) regionObj;

                //Select a random date range between base-date and the current date
                Instant instant = Instant.parse(baseDate); //"2000-01-01T00:00:00.000Z"
                Date baseTimeStamp = Date.from(instant);

                long epTime = System.currentTimeMillis(); //nanotime is NOT necessarily epoch time so don't use it as a timestamp
                long startTime = System.nanoTime();

                //Get the APIs for the selected region
                ArrayList<Document> apis = new ArrayList<>();
                ArrayList<String> api_ids = new ArrayList<>();
                MongoCursor<Document> cursor = apiCollection.find(eq("deployments.region", region)).iterator();
                try {
                    while (cursor.hasNext()) {

                        Document api = (Document) cursor.next();
                        apis.add(api);
                        api_ids.add(api.getString("_id"));
                    }
                } finally {
                    cursor.close();
                }

                //For each API, get the metrics
                List<Document> aggPipeline = Arrays.asList(
                        new Document("$match",
                                new Document("$and", Arrays.asList(
                                        new Document("apiDetails.appname", new Document("$in", api_ids)),
                                        new Document("$expr",
                                                new Document("$and", Arrays.asList(
                                                        new Document("$gte", Arrays.asList("$creationDate", baseTimeStamp)),
                                                        new Document("$lte", Arrays.asList("$creationDate", "$$NOW")))))))),
                        new Document("$group",
                                new Document("_id", "$apiDetails.appname")
                                        .append("totalVolume",
                                                new Document("$sum", "$transactionVolume"))
                                        .append("totalError",
                                                new Document("$sum", "$errorCount"))
                                        .append("totalSuccess",
                                                new Document("$sum", "$successCount"))),
                        new Document("$project",
                                new Document("aggregatedResponse",
                                        new Document("totalTransactionVolume", "$totalVolume")
                                                .append("errorRate",
                                                        new Document("$cond", Arrays.asList(new Document("$eq", Arrays.asList("$totalVolume", 0L)), 0L,
                                                                new Document("$multiply", Arrays.asList(new Document("$divide", Arrays.asList("$totalError", "$totalVolume")), 100L)))))
                                                .append("successRate",
                                                        new Document("$cond", Arrays.asList(new Document("$eq", Arrays.asList("$totalVolume", 0L)), 0L,
                                                                new Document("$multiply", Arrays.asList(new Document("$divide", Arrays.asList("$totalSuccess", "$totalVolume")), 100L))))))));

                metricsCollection.aggregate(aggPipeline).forEach(doc -> {
                    for (int i = 0; i < apis.size(); i++) {
                        Document api = apis.get(i);
                        if (((String) api.get("_id")).equals((String) doc.get("_id"))) {
                            api.append("results", doc);
                            break;
                        }
                    }
                    ;
                });


                long endTime = System.nanoTime();
                long duration = (endTime - startTime) / 1000000; // Milliseconds
                Document results = new Document();
                results.put("startTime", epTime);
                results.put("duration", duration);
                results.put("test", this.name());
                results.put("subtest", "USEMULTIQUERY");
                results.put("region", region);
                results.put("startDate", baseTimeStamp);
                times[currentTest] = results;
                currentTest++;
            }
        }
        return times;
    }

    private Document[] getSingleCollectionResults(int opsToTest, JSONObject testOptions, boolean warmup) {

        JSONArray regions = (JSONArray)((JSONObject)testOptions.get("custom")).get("regions");
        String baseDate = (String)((JSONObject)testOptions.get("custom")).get("baseDate");
        String metricsCollectionName = (String)customArgs.get("metricsCollectionName");

        Document[] times = new Document[opsToTest * regions.size()];
        int currentTest = 0;

        for (int o = 0; o < opsToTest; o++) {

            for(Object regionObj : regions) {

                String region = (String)regionObj;

                Instant instant = Instant.parse(baseDate); //"2000-01-01T00:00:00.000Z"
                Date baseTimeStamp = Date.from(instant);

                Bson filter = and(Arrays.asList(eq("deployments.region", region), or(Arrays.asList(and(Arrays.asList(gte("creationDate",
                        baseTimeStamp), lt("creationDate",
                        new java.util.Date()))), exists("creationDate", false)))));
                Bson sort = eq("_id", 1L);

                long epTime = System.currentTimeMillis(); //nanotime is NOT necessarily epoch time so don't use it as a timestamp
                long startTime = System.nanoTime();

                MongoCursor<Document> cursor = singleCollection.find(filter).sort(sort).iterator();
                String api_id = "";
                Long tCount = 0L;
                Long fCount = 0L;
                Long sCount = 0L;
                Document api = null;
                ArrayList<Document> apis = new ArrayList<>();
                try {
                    while (cursor.hasNext()) {
                        Document newdoc = (Document) cursor.next();
                        if (!api_id.equals(((Document) newdoc.get("apiDetails")).getString("appname"))) {
                            //We've advanced to records for a new API
                            //Check if this is the first in the result set
                            if (!api_id.equals("")) {
                                //Calculate the success and error rates
                                Document aggregatedResponse = new Document();
                                aggregatedResponse.append("totalTransactionVolume", tCount);
                                if (tCount == 0) {
                                    aggregatedResponse.append("errorRate", 0L);
                                    aggregatedResponse.append("successRate", 0L);
                                } else {
                                    Long successRate = ((sCount * 100L) / tCount);
                                    Long errorRate = ((fCount * 100L) / tCount);
                                    aggregatedResponse.append("errorRate", errorRate);
                                    aggregatedResponse.append("successRate", successRate);
                                }
                                Document results = new Document();
                                results.append("aggregatedResponse", aggregatedResponse);
                                api.append("results", results);
                                apis.add(api);
                            }
                            tCount = 0L;
                            fCount = 0L;
                            sCount = 0L;
                            api_id = ((Document) newdoc.get("apiDetails")).getString("appname");
                            api = newdoc;
                        } else {
                            //This is a record for the existing API - update the transaction/success/error counts
                            Integer newTransactions = newdoc.getInteger("transactionVolume");
                            if (newTransactions != null) {
                                tCount += newTransactions;
                            }
                            Integer newErrors = newdoc.getInteger("errorCount");
                            if (newErrors != null) {
                                fCount += newErrors;
                            }
                            Integer newSuccess = newdoc.getInteger("successCount");
                            if (newSuccess != null) {
                                sCount += newSuccess;
                            }
                        }
                    }
                    //Remember to add the final API to the list
                    if (api != null) {
                        Document aggregatedResponse = new Document();
                        aggregatedResponse.append("totalTransactionVolume", tCount);
                        if (tCount == 0) {
                            aggregatedResponse.append("errorRate", 0L);
                            aggregatedResponse.append("successRate", 0L);
                        } else {
                            Long successRate = ((sCount * 100L) / tCount);
                            Long errorRate = ((fCount * 100L) / tCount);
                            aggregatedResponse.append("errorRate", errorRate);
                            aggregatedResponse.append("successRate", successRate);
                        }
                        Document results = new Document();
                        results.append("aggregatedResponse", aggregatedResponse);
                        api.append("results", results);
                        apis.add(api);
                    }
                } finally {
                    cursor.close();
                }

                long endTime = System.nanoTime();
                long duration = (endTime - startTime) / 1000000; // Milliseconds
                Document results = new Document();
                results.put("startTime", epTime);
                results.put("duration", duration);
                results.put("test", this.name());
                results.put("subtest", "USESINGLECOLLECTION");
                results.put("region", region);
                results.put("startDate", baseTimeStamp);
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
}
