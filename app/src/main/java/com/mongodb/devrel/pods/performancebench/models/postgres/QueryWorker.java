/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mongodb.devrel.pods.performancebench.models.postgres;

/**
 *
 * @author graeme
 */
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryWorker implements Callable<List<TableRow>> {

    Logger logger;
    Statement stmt;
    String query;
    List<TableRow> results;
    /* Simulates N devices inserting X Documents */

    QueryWorker(Statement c, String q) {
            stmt = c;
            logger = LoggerFactory.getLogger(QueryWorker.class);
            this.query = q;
            results = new ArrayList<>();
    }

    @Override
    public List<TableRow> call() {
        try{
            List<TableRow> table = new ArrayList<>();
            ResultSet rs = stmt.executeQuery(query);
            TableRow.formTable(rs, table);
            return table;
        } catch (SQLException e) {
            logger.error("Error querying Postgres");
            logger.error(e.getMessage());
            System.exit(1);
            return null;
        }
    }
}
