package com.findwise.hydra;

import java.io.IOException;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.memorydb.MemoryConnector;
import com.findwise.hydra.memorydb.MemoryType;
import com.findwise.hydra.mongodb.MongoConnector;
import com.findwise.hydra.mongodb.MongoType;
import com.findwise.hydra.net.HttpRESTHandler;
import com.findwise.hydra.net.RESTServer;

public final class Main {

	private Main() {
	}

	private static Logger logger = LoggerFactory.getLogger(Main.class);

	public static void main(String[] args) {
		if (args.length > 0) {
			logger.error("Some parameters were provided on CommandLine. Ignored.");
		}

		CoreConfiguration conf = getConfiguration();
		
		DatabaseConnector<MongoType> backing = new MongoConnector(conf);
		DatabaseConnector<MemoryType> cache = new MemoryConnector();
		
		NodeMaster nm = new NodeMaster(conf, new CachingDatabaseConnector<MongoType, MemoryType>(backing, cache), new Pipeline());
		RESTServer server = new RESTServer(conf, new HttpRESTHandler<DatabaseType>(nm.getDatabaseConnector()));

		if (!server.blockingStart()) {
			if (server.hasError()) {
				logger.error("Failed to start REST server: ", server.getError());
			}
			else {
				logger.error("Failed to start REST server");
			}
			try {
				server.shutdown();
			}
			catch (IOException e2) {
				logger.error("IOException caught while shutting down REST server thread", e2);
				System.exit(1);
			}
			return;
		}

		try {
			nm.blockingStart();
		}
		catch (IOException e) {
			logger.error("Unable to start nodemaster... Shutting down.");
			try {
				server.shutdown();
			}
			catch (IOException e2) {
				logger.error("IOException caught while shutting down", e2);
				System.exit(1);
			}
		}
	}
	
	protected static CoreConfiguration getConfiguration() {
		try {
			return new FileConfiguration();
		}
		catch(ConfigurationException e) {
			logger.error("Unable to read configuration", e);
			return null;
		}
	}

}
