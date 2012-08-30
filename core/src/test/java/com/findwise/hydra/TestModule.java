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
		bind(DatabaseConnector.class).to(MongoConnector.class);
		bind(DatabaseConfiguration.class).to(CoreConfiguration.class);
	}
	
	@Provides @Singleton
	protected CoreConfiguration getCoreConfiguration() {
		MapConfiguration mc = new MapConfiguration();
		mc.setDatabaseUrl("127.0.0.1");
		mc.setRestPort(12001);
		mc.setNamespace(namespace);
		mc.setOldMaxCount(1000);
		mc.setOldMaxSize(10);
		
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
	protected StoredPipeline getStoredPipeline() {
		try {
			return new StoredPipeline(CoreModule.PIPELINE_DIRECTORY);
		} catch (IOException e) {
			return null;
		}
	}
}
