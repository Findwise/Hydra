package com.findwise.hydra.mongodb;

import com.findwise.hydra.DatabaseConfiguration;

public class DatabaseConfigurationFactory {
	public static DatabaseConfiguration getDatabaseConfiguration(final String namespace) {
		MongoConfiguration config = new MongoConfiguration();
		config.setNamespace(namespace);
		return config;
	}

}
