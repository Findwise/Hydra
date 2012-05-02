package com.findwise.hydra;

import java.io.IOException;

import com.findwise.hydra.mongodb.MongoConnector;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
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
		CoreConfiguration c = getCoreConfiguration();
		bindConstant().annotatedWith(Names.named(CoreConfiguration.NAMESPACE_PARAM)).to(c.getNamespace());
		bindConstant().annotatedWith(Names.named(CoreConfiguration.DATABASE_URL_PARAM)).to(c.getDatabaseUrl());
		bindConstant().annotatedWith(Names.named(CoreConfiguration.POLLING_INTERVAL_PARAM)).to(c.getPollingInterval());
		bindConstant().annotatedWith(Names.named(CoreConfiguration.REST_PORT_PARAM)).to(c.getRestPort());
		
		bind(DatabaseConnector.class).to(MongoConnector.class);
	}
	
	@Provides @Singleton
	protected CoreConfiguration getCoreConfiguration() {
		MapConfiguration mc = new MapConfiguration();
		mc.setDatabaseUrl("127.0.0.1");
		mc.setRestPort(12001);
		mc.setNamespace(namespace);
		return mc;
	}
	
	/*@Provides
	protected DatabaseConnector getConnector() {
		Injector inj = Guice.createInjector(this);
		return inj.getInstance(DatabaseConnector.class);
	}*/
	
	@Provides @Singleton
	protected Configuration getConfiguration(CoreConfiguration core) {
		return core;
	}
	
	@Provides
	protected StoredPipeline getStoredPipeline(@Named(DatabaseConnector.NAMESPACE_PARAM) String namespace) {
		try {
			return new StoredPipeline(CoreModule.PIPELINE_DIRECTORY);
		} catch (IOException e) {
			return null;
		}
	}
}
