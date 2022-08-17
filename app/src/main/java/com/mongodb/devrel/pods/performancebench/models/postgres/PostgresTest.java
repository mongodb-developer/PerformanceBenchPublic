/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mongodb.devrel.pods.performancebench.models.postgres;

/**
 *
 * @author graeme
 */
import com.mongodb.devrel.pods.performancebench.utilities.BulkLoadPG;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.BatchUpdateException;
import java.sql.Timestamp;

import com.mongodb.devrel.pods.performancebench.SchemaTest;
import com.mongodb.devrel.pods.performancebench.utilities.RecordFactory;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.Date;


import org.bson.Document;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresTest implements SchemaTest {

    Logger logger;
    Connection pgClient;
    String rawCollectionName;
    String dbName;
    RecordFactoryTmp recordFactory = new RecordFactoryTmp();
    JSONObject args;
    JSONObject customArgs;
    private static final Random random = new Random();
    ExecutorService executorService;
    

    public PostgresTest() {
        logger = LoggerFactory.getLogger(PostgresTest.class);
    }
    
    @Override
    public void initialize(JSONObject newArgs){
        
        this.args = newArgs;
        this.customArgs = (JSONObject)args.get("custom");
        this.executorService = Executors.newFixedThreadPool(15);
        
        
        try {
            Class.forName("org.postgresql.Driver");
            pgClient = DriverManager
                    .getConnection(customArgs.get("uri").toString(), customArgs.get("pguser").toString(), customArgs.get("pgpass").toString());
            if(pgClient.isValid(5)){
                logger.info("Opened connection to Postgres successfully");
            }else{
                logger.error("Error connecting to Postgres - connection is invalid");
                System.exit(1);
            }
        } catch (ClassNotFoundException | SQLException e) {
            logger.error("Error connecting to Postgres");
            logger.error(e.getMessage());
            System.exit(1);
        }
        
        RecordFactory factory = new RecordFactory();

        BulkLoadPG bulkLoad = new BulkLoadPG(pgClient, factory);

        if ((Boolean)customArgs.get("loadData")) {
            bulkLoad.loadInitialData(customArgs);
            prepareTestData();
        }

    }
    
    
    /* Simulates N devices inserting X Documents */
    

    // Method to take the Bulk loaded data and prepare it for testing
    // For exmaple to reshape or index it
    private void prepareTestData() {
        // For now we do nothing for Postgres test we just use as bulk loaded

    }

    @Override
    public String name() {
        return "PostgresTest";
    }

    @Override
    public void warmup() {
        // Preload each table to pull it all into cache if it can
        //Requires POstgres 9.4 or later, plus a one-time run of 'CREATE EXTENSION pg_prewarm'
        //Instructions on CREATE EXTENSION at https://stackoverflow.com/questions/3862648/how-to-use-install-dblink-in-postgresql/13264961#13264961 
        //Be careful to apply to correct schema. I added the command to resetSchema.sql
        //If it doesnt fit then we will part fill the cache;
        
        String[] tabletypes = { "orderstable", "orderitemstable", "invoicestable", "shipmentstable", "shipmentitemstable", "productstable", "warehousestable", "customerstable" };
        
        try{
            for (String tname : tabletypes) {
                String PRELOAD_SQL = "SELECT " + (String)customArgs.get("pgSchema") + ".pg_prewarm('" + (String)customArgs.get("pgSchema") + "." 
                        + (String)customArgs.get(tname) 
                        + "');";
                Statement stmt = pgClient.createStatement();
                stmt.executeQuery(PRELOAD_SQL);
            }
        } catch (SQLException e) {
            logger.error("Error preloading Postgres data");
            logger.error(e.getMessage());
            System.exit(1);
        }
    }

    @Override
    public void cleanup() {
        executorService.shutdown();
    }
    
        @Override
    public double[] executeMeasure(int opsToTest, String subtest, JSONObject args, boolean warmup){
        
        return switch (subtest) {
            case "GETORDERBYID" -> getOrdersByIdTest(opsToTest, args, warmup);
            case "ADDSHIPMENT" -> addShipmentsTest(opsToTest, args, warmup);
            case "INCITEMCOUNT" -> intItemCountTest(opsToTest, args, warmup);
            case "INCMULTIITEM" -> intItemCountTestWithDate(opsToTest, args, warmup);
            default -> null;
        };
    }
    
    
    private double[] getOrdersByIdTest(int opsToTest, JSONObject testOptions, boolean warmup) {

        int customers = ((Long)((JSONObject)testOptions.get("custom")).get("customers")).intValue();
        int orders = ((Long)((JSONObject)testOptions.get("custom")).get("orders")).intValue();
        
        double[] times = new double[opsToTest];
        
        for (int o = 0; o < opsToTest; o++) {
            int custid = random.nextInt(customers) + 1;
            int orderid = random.nextInt(orders) + 1;
            long startTime = System.nanoTime();
            getOrderById(custid, orderid);
            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000; // Microseconds
            times[o] = duration;
        }
        return times;
    }
    
    public List<TableRow> getOrderById(int customerId, int orderId) {
            
        try{
            
            List<TableRow> rval = new ArrayList<>();
            List<Future<List<TableRow>>> partials = new ArrayList<>();
            
            String[] ordertables = {"orderstable", "orderitemstable", "invoicestable", "shipmentstable", "shipmentitemstable"};
            String orderPrefix = String.format("C#%d#O#%d", customerId, orderId);
            
            for (String tname : ordertables) {
                String QUERY_SQL = "SELECT * FROM \"" + (String)customArgs.get("pgSchema") + "\".\""
                        + (String)customArgs.get(tname) + "\" "
                        + "WHERE \"_id\"  >= '" + orderPrefix + "' "
                        + "AND \"_id\" <= '" + orderPrefix + "$';";
                Statement stmt = pgClient.createStatement();
                Future<List<TableRow>> future = executorService.submit(new QueryWorker(stmt, QUERY_SQL));
                partials.add(future);
            }
            for (Future<List<TableRow>> p : partials) {
                rval.addAll(p.get());
            }
            return rval;
        } catch (InterruptedException | SQLException | ExecutionException e) {
            logger.error("Error running Postgres getOrderById");
            logger.error(e.getMessage());
            System.exit(1);
            return null;
        } 
    }
    
    
    private double[] addShipmentsTest(int opsToTest, JSONObject testOptions, boolean warmup) {

        int customers = ((Long)((JSONObject)testOptions.get("custom")).get("customers")).intValue();
        int orders = ((Long)((JSONObject)testOptions.get("custom")).get("orders")).intValue();
        int warehouses = ((Long)((JSONObject)testOptions.get("custom")).get("warehouses")).intValue();
        int shipmentitems = ((Long)((JSONObject)testOptions.get("custom")).get("shipmentitems")).intValue();
        
        double[] times = new double[opsToTest];
                
        for (int o = 0; o < opsToTest; o++) {
            int custid = random.nextInt(customers) + 1;
            int orderid = random.nextInt(orders) + 1;
            int warehouseid = random.nextInt(warehouses) + 1;
            int itemsinshipment = random.nextInt(shipmentitems) + 1;
            long startTime = System.nanoTime();
            addNewShipment(custid, orderid, 1, itemsinshipment, warehouseid);
            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000; // Microseconds
            times[o] = duration;
        }
        return times;
    }
    
    
    // Greates a new shipment for all of the items in this order
    // Don't worry if they were already shipped - imagine they got lost
    // we only want to test the write so lets assume we already have the order doc
    // And select N items to ship where 1<N<NitemsMax
    public int addNewShipment(int custid, int orderid, int shipmentid, int itemsinshipment, int warehouseid) {

        int ndocs = 0;
        
        try{
            
            //pgClient.setAutoCommit(false);

            String INSERT_SQL = "INSERT INTO " + (String)customArgs.get("pgSchema") + "." 
                    + (String)customArgs.get("shipmentstable") 
                    + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

            PreparedStatement preparedStatement = pgClient.prepareStatement(INSERT_SQL);
            
            String ITEMS_SQL = "INSERT INTO " + (String)customArgs.get("pgSchema") + "." 
                    + (String)customArgs.get("shipmentitemstable") 
                    + " VALUES (?, ?, ?, ?, ?, ?);";
            
            PreparedStatement itemsPreparedStatement = pgClient.prepareStatement(ITEMS_SQL);
            
            boolean keyAccepted;
            do{
                keyAccepted = true;
                Document shipment = recordFactory.getShipment(custid, orderid, shipmentid, warehouseid);

                String shipmentPK = String.format("C#%d#O#%d#S#%d", shipment.getInteger("customerId"),
                    shipment.getInteger("orderId"), shipment.getInteger("shipmentId"));
                shipment.put("_id", shipmentPK);

                preparedStatement.setString(1, shipment.getString("_id"));
                preparedStatement.setInt(2, shipment.getInteger("customerId"));
                preparedStatement.setInt(3, shipment.getInteger("orderId"));
                preparedStatement.setInt(4, shipment.getInteger("shipmentId"));
                preparedStatement.setString(5, shipment.getString("type"));
                preparedStatement.setTimestamp(6, new Timestamp(shipment.getDate("date").getTime()));
                preparedStatement.setString(7, shipment.getEmbedded(Arrays.asList(new String[]{"shipTo", "Country"}), String.class));
                preparedStatement.setString(8, shipment.getEmbedded(Arrays.asList(new String[]{"shipTo", "County"}), String.class));
                preparedStatement.setString(9, shipment.getEmbedded(Arrays.asList(new String[]{"shipTo", "City"}), String.class));
                preparedStatement.setString(10, shipment.getEmbedded(Arrays.asList(new String[]{"shipTo", "Street"}), String.class));
                preparedStatement.setInt(11, shipment.getEmbedded(Arrays.asList(new String[]{"shipTo", "Number"}), Integer.class));
                preparedStatement.setInt(12, shipment.getEmbedded(Arrays.asList(new String[]{"shipTo", "ZipCode"}), Integer.class));
                preparedStatement.setString(13, shipment.getString("method"));
                preparedStatement.addBatch();
                
                try{
                    int[] res = preparedStatement.executeBatch();
                    for(int i : res){
                        ndocs += i;
                    }
                } catch (BatchUpdateException  e) {
                    if (e.getMessage().contains("duplicate key")){
                        //Really bad practise to catch an exception as a legitimate code path, but possibly easier than
                        //querying for next available shipment number - keep iterating until we don't get a duplicate PK error.
                        keyAccepted = false;
                        shipmentid++;
                    }else{
                        throw(e);
                    }
                }
            }while(keyAccepted == false);
                
 
            int fromItem = 1;
            for (int si = fromItem; si <= fromItem + itemsinshipment; si++) {
                keyAccepted = true;
                Document shippedItem = recordFactory.getShipItem(custid, orderid, shipmentid, si);
                String shippedItemPK = String.format("C#%d#O#%d#S#%d#I%d", shippedItem.getInteger("customerId"),
                        shippedItem.getInteger("orderId"),
                        shippedItem.getInteger("shipmentId"),
                        shippedItem.getInteger("shipmentItemId"));
                shippedItem.put("_id", shippedItemPK);

                itemsPreparedStatement.setString(1, shippedItem.getString("_id"));
                itemsPreparedStatement.setInt(2, shippedItem.getInteger("customerId"));
                itemsPreparedStatement.setInt(3, shippedItem.getInteger("orderId"));
                itemsPreparedStatement.setInt(4, shippedItem.getInteger("shipmentId"));
                itemsPreparedStatement.setInt(5, shippedItem.getInteger("shipmentItemId"));
                itemsPreparedStatement.setString(6, shippedItem.getString("type"));
                itemsPreparedStatement.addBatch(); 
                int[] res = itemsPreparedStatement.executeBatch();
                for(int i : res){
                    ndocs += i;
                }
            }
            //pgClient.commit();
            //pgClient.setAutoCommit(true);
        } catch (BatchUpdateException batchUpdateException) {
            logger.error("Error inserting new shipment Postgres data");
            logger.error(batchUpdateException.getMessage());
            System.exit(1);
        } catch (SQLException e) {
            logger.error("Error inserting new shipment Postgres data");
            logger.error(e.getMessage());
            System.exit(1);
        }
            
        return ndocs;
    }
    
    
    private double[] intItemCountTest(int opsToTest, JSONObject testOptions, boolean warmup) {

        int customers = ((Long)((JSONObject)testOptions.get("custom")).get("customers")).intValue();
        int orders = ((Long)((JSONObject)testOptions.get("custom")).get("orders")).intValue();
        int items = ((Long)((JSONObject)testOptions.get("custom")).get("items")).intValue();
        
        double[] times = new double[opsToTest];
        
        for (int o = 0; o < opsToTest; o++) {
            int custid = random.nextInt(customers) + 1;
            int orderid = random.nextInt(orders) + 1;
            int itemid = random.nextInt(items / 2) + 1; // By selecting a random number up to
            // half items its more likely to be
            // there.
            long startTime = System.nanoTime();
            updateSingleItem(custid, orderid, itemid);
            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000; // Microseconds
            times[o] = duration;
        }
        return times;
    }

    // Increase quantity for a single item already in the order    
    public int updateSingleItem(int custid, int orderid, int itemid) {
        
        try{
        
            String orderPK = String.format("C#%d#O#%d#I#%d", custid, orderid, itemid);

            String UPDATE_SQL = "UPDATE " + (String)customArgs.get("pgSchema") + "." 
            + (String)customArgs.get("orderitemstable") 
            + " SET qty = qty + 1" 
            + " WHERE \"_id\" = ?;";

            PreparedStatement stmt = pgClient.prepareStatement(UPDATE_SQL);
            stmt.setString(1, orderPK);

            return stmt.executeUpdate();
            
        }
        catch (SQLException e) {
            logger.error("Error updating single item in Postgres data");
            logger.error(e.getMessage());
            System.exit(1);
            return -1;
        }
    }
        

    private double[] intItemCountTestWithDate(int opsToTest, JSONObject testOptions, boolean warmup) {

        int customers = ((Long)((JSONObject)testOptions.get("custom")).get("customers")).intValue();
        int orders = ((Long)((JSONObject)testOptions.get("custom")).get("orders")).intValue();
        int items = ((Long)((JSONObject)testOptions.get("custom")).get("items")).intValue();
        
        double[] times = new double[opsToTest];
        
        for (int o = 0; o < opsToTest; o++) {
            int custid = random.nextInt(customers) + 1;
            int orderid = random.nextInt(orders) + 1;
            int itemid = random.nextInt(items / 2) + 1; // By selecting a random number up to
            // half items its more likely to be
            // there.
            long startTime = System.nanoTime();
            updateMultiItem(custid, orderid, itemid);
            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000; // Microseconds
            times[o] = duration;
        }
        return times;
    }    
    
    public int updateMultiItem(int custid, int orderid, int itemid) {
        
        

        //Update the item quantity
        int ret = updateSingleItem(custid, orderid, itemid);
        
        
        try{
        
            String orderPK = String.format("C#%d#O#%d", custid, orderid);
            
            String UPDATE_SQL = "UPDATE " + (String)customArgs.get("pgSchema") + "." 
            + (String)customArgs.get("orderstable") 
            + " SET lastupdate = ?" 
            + " WHERE \"_id\" = ?;";

            PreparedStatement stmt = pgClient.prepareStatement(UPDATE_SQL);
            stmt.setTimestamp(1, new Timestamp(new Date().getTime()));
            stmt.setString(2, orderPK);

            return ret + stmt.executeUpdate();
            
        }
        catch (SQLException e) {
            logger.error("Error updating single item in Postgres data");
            logger.error(e.getMessage());
            System.exit(1);
            return -1;
        }

    }

}
