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

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.util.StatusPrinter;
import org.apache.commons.cli.ParseException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;

public class PerformanceBench {

    static final String version = "PerformanceBench 0.0.3";
    static Logger logger;

    public static void main(String[] args) {

        //Application config options will be read from a JSON file specified as
        //a command line parameter in to this object which is an extension of the
        //BSON document class.
        //See the README for details of the config file format.
        BenchOptions options = null;

        logger = LoggerFactory.getLogger(PerformanceBench.class);
        logger.info(version);


        //Uncomment the following lines for debugging info if there is a problem with logback.xml -
        //LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        //// print logback's internal status
        //StatusPrinter.print(lc);

        //Attempt to read the JSON config file.
        try {
            options = new BenchOptions(args);
        } catch (ParseException e) {
            logger.error(e.getMessage());
            System.exit(1);
        } catch (FileNotFoundException e) {
            logger.error("Can't open config file");
            logger.error(e.getMessage());
            System.exit(1);
        } catch (IOException e) {
            logger.error("Issue in reading config file");
            logger.error(e.getMessage());
            System.exit(1);
        } catch (org.json.simple.parser.ParseException e) {
            logger.error("Issue in parsing JSON in config file");
            logger.error(e.getMessage());
            System.exit(1);
        }


        //For each model specified in the config file, attempt to instantiate an instance of the
        //corresponding class and execute its measures.
        for (Iterator<JSONObject> it = ((JSONArray)options.get("models")).iterator(); it.hasNext();) {
            JSONObject modelArgs = it.next();
            if((Long)modelArgs.get("iterations") > 0) {
                SchemaTest st = null;
                try {
                    Class<?> clazz = Class.forName(modelArgs.get("namespace").toString() + "." + modelArgs.get("className").toString());
                    Object object = clazz.getDeclaredConstructor().newInstance();
                    st = (SchemaTest) object;
                } catch (ClassNotFoundException | InstantiationException | IllegalAccessException |
                         NoSuchMethodException | SecurityException | InvocationTargetException e) {
                    logger.error("Issue instantiating SchemaTest Object " + modelArgs.get("className").toString());
                    logger.error(e.getMessage());
                    System.exit(1);
                }
                //Execute the model class' initialize method. This is intended to carry out steps such as connection pool initialization
                //and test data loading / preparation.
                st.initialize(modelArgs);
                TestRunner runner = new TestRunner();
                logger.info("Running tests...");
                //Execute each measure defined by the model.
                runner.runTest(st, modelArgs);
                //Execute the model's cleanup method. This is intended for tasks such as connection pool disconnects and test data removal
                //or reset if appropriate, but can also be used to perform any necessary calculations on the results' data, such as aggregations,
                //calculations of averages / means / percentile values etc.
                st.cleanup(modelArgs);
            }
        }

    }

}

