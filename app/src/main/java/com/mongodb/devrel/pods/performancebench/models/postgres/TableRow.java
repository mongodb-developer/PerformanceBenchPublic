/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mongodb.devrel.pods.performancebench.models.postgres;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author graeme
 * 
 * Based on https://stackoverflow.com/a/31413263
 * 
 */
public class TableRow {
    public List<Entry<Object, Class>> row;
    public static final Map<String, Class> TYPE;
    Logger logger;

    static {
        TYPE = new HashMap<>();

        TYPE.put("INTEGER", Integer.class);
        TYPE.put("INT4", Integer.class);
        TYPE.put("TINYINT", Byte.class);
        TYPE.put("SMALLINT", Short.class);
        TYPE.put("BIGINT", Long.class);
        TYPE.put("REAL", Float.class);
        TYPE.put("FLOAT", Double.class);
        TYPE.put("DOUBLE", Double.class);
        TYPE.put("DECIMAL", BigDecimal.class);
        TYPE.put("NUMERIC", BigDecimal.class);
        TYPE.put("BOOLEAN", Boolean.class);
        TYPE.put("CHAR", String.class);
        TYPE.put("VARCHAR", String.class);
        TYPE.put("LONGVARCHAR", String.class);
        TYPE.put("DATE", Date.class);
        TYPE.put("TIME", Time.class);
        TYPE.put("TIMESTAMP", Timestamp.class);
        TYPE.put("SERIAL",Integer.class);
    }

    public TableRow() {
        row = new ArrayList<>();
        logger = LoggerFactory.getLogger(TableRow.class);
    }

    public <T> void add(T data) {
        row.add(new AbstractMap.SimpleImmutableEntry<>(data, data.getClass()));
    }

    public void add(Object data, String sqlType) {
        Class castType = TableRow.TYPE.get(sqlType.toUpperCase());
        try {
            this.add(castType.cast(data));
        } catch (NullPointerException e) {
            logger.error("Error mapping Postgres row to list");
            logger.error(e.getMessage());
            System.exit(1);
        }
    }

    public static void formTable(ResultSet rs, List<TableRow> table) throws SQLException {
        if (rs == null)
            return;

        ResultSetMetaData rsmd;
        try {
            rsmd = rs.getMetaData();

            int NumOfCol = rsmd.getColumnCount();

            while (rs.next()) {
                TableRow current_row = new TableRow();

                for (int i = 1; i <= NumOfCol; i++) {
                    Object data = rs.getObject(i);
                    current_row.add(data, rsmd.getColumnTypeName(i));
                }

                table.add(current_row);
            }
        } catch (SQLException e) {
            Logger l = LoggerFactory.getLogger(TableRow.class);
            l.error("Error mapping Postgres row to list");
            l.error(e.getMessage());
            System.exit(1);
        }
    }
}
