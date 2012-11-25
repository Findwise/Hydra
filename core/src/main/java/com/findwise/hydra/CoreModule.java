package com.findwise.hydra;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.mongodb.MongoConnector;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

public class CoreModule extends AbstractModule {
	public static final String PIPELINE_DIRECTORY = "work";
	
	private static Logger logger = LoggerFactory.getLogger(CoreModule.class);
	private boolean propertiesFileNameProvided;
	private String propertiesFileName;

	public CoreModule(){
		propertiesFileNameProvided = false;
	}

	public CoreModule(String propertiesFileName) {
		propertiesFileNameProvided = true;
		this.propertiesFileName = propertiesFileName;
	}

	@Override
	protected void configure() {
		bind(DatabaseConnector.class).to(MongoConnector.class);
		bind(DatabaseConfiguration.class).to(CoreConfiguration.class);
	}
	
	@Provides
	@Singleton
	protected CoreConfiguration getConfiguration() {
		try {
			if (propertiesFileNameProvided) {
				return new FileConfiguration(propertiesFileName);
			} else {
				return new FileConfiguration();
			}
		}
		catch(ConfigurationException e) {
			logger.error("Unable to read configuration", e);
			return null;
		}
	}
}
