package com.mongodb.devrel.pods.performancebench;

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

/**
 **
 * Class to read and hold Performance Bench configuration options as a BSON document.
 *
 * Path to a JSON document holding the configuration options is passed to the application
 * as a command-line parameter. The JSON file is expected to have a single top level field - an array
 * named "models". Each entry in the array is a document describing a set of tests to be performed
 * by PerformanceBench. See the project README for a full description of the expected JSON format.
 *
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

        Options cliopt = new Options();
        cliopt.addOption("c", "config", true, "Config file with parameters");

        CommandLine cmd;
        try {
            cmd = parser.parse(cliopt, args);
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("PerformanceBench", cliopt);
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

