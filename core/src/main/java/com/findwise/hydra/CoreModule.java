package com.findwise.hydra;

import java.io.IOException;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.mongodb.MongoConnector;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

public class CoreModule extends AbstractModule {
	public static final String PIPELINE_DIRECTORY = "work";
	
	private static Logger logger = LoggerFactory.getLogger(CoreModule.class);

	@Override
	protected void configure() {
		CoreConfiguration c = getConfiguration();
		
		bindConstant().annotatedWith(Names.named(CoreConfiguration.NAMESPACE_PARAM)).to(c.getNamespace());
		bindConstant().annotatedWith(Names.named(CoreConfiguration.DATABASE_URL_PARAM)).to(c.getDatabaseUrl());
		bindConstant().annotatedWith(Names.named(CoreConfiguration.POLLING_INTERVAL_PARAM)).to(c.getPollingInterval());
		bindConstant().annotatedWith(Names.named(CoreConfiguration.REST_PORT_PARAM)).to(c.getRestPort());
		bindConstant().annotatedWith(Names.named(DatabaseConnector.DATABASE_USER)).to(c.getDatabaseUser());
		bindConstant().annotatedWith(Names.named(DatabaseConnector.DATABASE_PASSWORD)).to(c.getDatabasePassword());

		bind(DatabaseConnector.class).to(MongoConnector.class);
	}
	
	@Provides
	@Singleton
	protected CoreConfiguration getConfiguration() {
		try {
			return new FileConfiguration();
		}
		catch(ConfigurationException e) {
			logger.error("Unable to read configuration", e);
			return null;
		}
	}
	
	@Provides
	protected StoredPipeline getStoredPipeline(@Named(DatabaseConnector.NAMESPACE_PARAM) String namespace) {
		try {
			return new StoredPipeline(PIPELINE_DIRECTORY);
		} catch (IOException e) {
			logger.error("Pipeline threw IOException", e);
			return null;
		}
	}
}
