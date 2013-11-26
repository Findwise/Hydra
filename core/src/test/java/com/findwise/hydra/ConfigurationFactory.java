package com.findwise.hydra;

import com.findwise.hydra.mongodb.MongoConfiguration;

public class ConfigurationFactory {
	public static CoreConfiguration getConfiguration(final String namespace) {
		CoreMapConfiguration mc = new CoreMapConfiguration(new MongoConfiguration(), new MapConfiguration());
		return mc;
		
	}
}
