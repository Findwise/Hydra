package com.findwise.hydra.output;

import com.findwise.hydra.Configuration;
import com.findwise.hydra.CoreConfiguration;
import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.MapConfiguration;
import com.findwise.hydra.mongodb.MongoConnector;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Names;

public class TestModule extends AbstractModule {
	String namespace;
	
	public TestModule() {
		this("junitspace");
	}
	
	public TestModule(String namespace) {
		this.namespace = namespace;
	}

	@Override
	protected void configure() {
		CoreConfiguration c = getConfiguration();
		bindConstant().annotatedWith(Names.named(DatabaseConnector.NAMESPACE_PARAM)).to(c.getNamespace());
		bindConstant().annotatedWith(Names.named(Configuration.POLLING_INTERVAL_PARAM)).to(c.getPollingInterval());
		bindConstant().annotatedWith(Names.named(DatabaseConnector.DATABASE_URL_PARAM)).to(c.getDatabaseUrl());
		bindConstant().annotatedWith(Names.named(Configuration.REST_PORT_PARAM)).to(c.getRestPort());
		
		bind(DatabaseConnector.class).to(MongoConnector.class);
	}
	
	@Provides @Singleton
	protected CoreConfiguration getConfiguration() {
		MapConfiguration mc = new MapConfiguration();
		mc.setDatabaseUrl("127.0.0.1");
		mc.setRestPort(12001);
		mc.setNamespace(namespace);
		return mc;
	}
}