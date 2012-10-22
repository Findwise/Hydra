package com.findwise.hydra;

import com.findwise.hydra.DatabaseConfiguration;
import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.mongodb.MongoConnector;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Names;

public class ExampleModule extends AbstractModule {
	private String namespace;
	private String mongohost;
	private String username;
	private String password;
	
	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}
	
	
	public ExampleModule() {
		this("pipeline", "127.0.0.1");
	}
	
	public ExampleModule(String namespace, String mongohost) {
		this(namespace, mongohost, "admin", "changeme");
	}
	
	public ExampleModule(String namespace, String mongohost, String username, String password) {
		this.namespace = namespace;
		this.mongohost = mongohost;
		this.username = username;
		this.password = password;
	}

	@Override
	protected void configure() {
		DatabaseConfiguration c = getConfiguration();
		
		bindConstant().annotatedWith(Names.named(DatabaseConnector.NAMESPACE_PARAM)).to(c.getNamespace());
		bindConstant().annotatedWith(Names.named(DatabaseConnector.DATABASE_URL_PARAM)).to(c.getDatabaseUrl());
		bindConstant().annotatedWith(Names.named(DatabaseConnector.DATABASE_USER)).to(c.getDatabaseUser());
		bindConstant().annotatedWith(Names.named(DatabaseConnector.DATABASE_PASSWORD)).to(c.getDatabasePassword());
		bindConstant().annotatedWith(Names.named(DatabaseConnector.OLD_MAX_COUNT)).to(c.getOldMaxCount());
		bindConstant().annotatedWith(Names.named(DatabaseConnector.OLD_MAX_SIZE_MB)).to(c.getOldMaxSize());

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
				return mongohost;
			}

			@Override
			public String getDatabaseUser() {
				return username;
			}

			@Override
			public String getDatabasePassword() {
				return password;
			}

			@Override
			public int getOldMaxSize() {
				return 200;
			}

			@Override
			public int getOldMaxCount() {
				return 2000;
			}
		};
	}
}
