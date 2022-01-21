package com.mongodb.jlp.orderbench;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Worker implements Runnable {

	Logger logger;

	/* Simulates N devices inserting X Documents */

	Worker() {
		logger = LoggerFactory.getLogger(Worker.class);
	}

	public void run() {

	}

}
