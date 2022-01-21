package com.mongodb.jlp.orderbench;

import org.apache.commons.cli.*;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestOptions extends Document {
    Logger logger;

    TestOptions(String[] args) throws ParseException {
        super();

        logger = LoggerFactory.getLogger(TestOptions.class);
        this.put("iterations", 5);

        CommandLineParser parser = new DefaultParser();

        Options cliopt;
        cliopt = new Options();
        cliopt.addOption("u", "uri", true, "MongoDB COnnection URI");
        cliopt.addOption("l", "load", false, "Perform Initial Data Load");
        cliopt.addOption("s", "size", true, "Size of orders");
        cliopt.addOption("n", "customers", true, "Number of customers");
        cliopt.addOption("m", "orders", true, "Number of orders per customer");
        cliopt.addOption("i", "items", true, "Number of items per order");
        cliopt.addOption("p", "products", true, "Number of products");

        CommandLine cmd;
        try {
            cmd = parser.parse(cliopt, args);
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("OrderBench", cliopt);
            throw e;
        }

        if (cmd.hasOption("uri")) {
            this.put("uri", cmd.getOptionValue("uri"));
        } else {
            this.put("uri", "mongodb://localhost:27017");
        }

        if (cmd.hasOption("load")) {
            this.put("load", true);
        }

        String[] integerParamaters = { "size", "customers", "orders", "items", "products" };
        Document integerDefaults = Document.parse("{size:4096,customers:10000,orders:20,items:5,products:10000}");

        for (String param : integerParamaters) {
            if (cmd.hasOption(param)) {
                this.put(param, Integer.parseInt(cmd.getOptionValue(param)));
            } else {
                this.put(param, integerDefaults.getInteger(param, 1));
            }
        }

    }

}
