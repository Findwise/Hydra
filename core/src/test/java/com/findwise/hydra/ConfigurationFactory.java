package com.findwise.hydra;

public class ConfigurationFactory {
	public static CoreConfiguration getConfiguration(final String namespace) {
		MapConfiguration mc = new MapConfiguration();
		mc.setDatabaseUrl("mongodb://localhost:27017");
		mc.setRestPort(12001);
		mc.setNamespace(namespace);
		mc.setOldMaxCount(1000);
		mc.setOldMaxSize(10);

		return mc;
		
	}
}
