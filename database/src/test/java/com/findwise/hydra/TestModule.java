package com.findwise.hydra;

import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.mongodb.MongoConnector;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Names;

public class TestModule extends AbstractModule {
	private String namespace;
	
	public TestModule() {
		this("junitspace");
	}
	
	public TestModule(String namespace) {
		this.namespace = namespace;
	}

	@Override
	protected void configure() {
		DatabaseConfiguration c = getConfiguration();
		bindConstant().annotatedWith(Names.named(DatabaseConnector.NAMESPACE_PARAM)).to(c.getNamespace());
		bindConstant().annotatedWith(Names.named(DatabaseConnector.DATABASE_URL_PARAM)).to(c.getDatabaseUrl());
		bindConstant().annotatedWith(Names.named(DatabaseConnector.DATABASE_USER)).to(c.getDatabaseUser());
		bindConstant().annotatedWith(Names.named(DatabaseConnector.DATABASE_PASSWORD)).to(c.getDatabasePassword());
		bind(DatabaseConnector.class).to(MongoConnector.class);
	}
	
	@Provides @Singleton
	protected DatabaseConfiguration getConfiguration() {
		return new DatabaseConfiguration() {
			
			@Override
			public String getNamespace() {
				return namespace;
			}
			
			@Override
			public String getDatabaseUrl() {
				return "127.0.0.1";
			}
			
			@Override
			public String getDatabaseUser() {
				return "admin";
			}
			
			@Override
			public String getDatabasePassword() {
				return "changeme";
			}
		};
	}
}
