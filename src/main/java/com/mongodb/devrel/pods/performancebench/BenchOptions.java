/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mongodb.devrel.pods.performancebench;

/**
 *
 * @author graeme
 */
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.cli.*;
import org.bson.Document;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchOptions extends Document {

    Logger logger;

    BenchOptions(String[] args) throws ParseException, FileNotFoundException, IOException, org.json.simple.parser.ParseException {
        super();

        logger = LoggerFactory.getLogger(BenchOptions.class);

        CommandLineParser parser = new DefaultParser();

        Options cliopt;
        cliopt = new Options();
        cliopt.addOption("c", "config", true, "Config file with parameters");

        CommandLine cmd;
        try {
            cmd = parser.parse(cliopt, args);
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("OrderBench", cliopt);
            throw e;
        }

        JSONObject configFileParams = new JSONObject();
        if (cmd.hasOption("config")) {
            JSONParser jsonparser = new JSONParser();
            Object parsedObj = jsonparser.parse(new FileReader(cmd.getOptionValue("config")));
            configFileParams = (JSONObject) parsedObj;
            logger.debug(configFileParams.toString());
        }

        this.put("models", configFileParams.get("models"));

    }

}

