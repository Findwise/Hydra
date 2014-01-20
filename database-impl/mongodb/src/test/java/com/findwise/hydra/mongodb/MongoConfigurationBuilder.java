package com.findwise.hydra.mongodb;

public class MongoConfigurationBuilder {

	private MongoConfiguration mongoConfiguration;

	public MongoConfigurationBuilder() {
		this.mongoConfiguration = new MongoConfiguration();
	}

	public MongoConfiguration build() {
		return mongoConfiguration;
	}

	public MongoConfigurationBuilder setNamespace(String namespace) {
		mongoConfiguration.setNamespace(namespace);
		return this;
	}

	public MongoConfigurationBuilder setDatabaseUrl(String value) {
		mongoConfiguration.setDatabaseUrl(value);
		return this;
	}

	public MongoConfigurationBuilder setDatabaseUser(String user) {
		mongoConfiguration.setDatabaseUser(user);
		return this;
	}

	public MongoConfigurationBuilder setDatabasePassword(String password) {
		mongoConfiguration.setDatabasePassword(password);
		return this;
	}

	public MongoConfigurationBuilder setOldMaxSize(int size) {
		mongoConfiguration.setOldMaxSize(size);
		return this;
	}

	public MongoConfigurationBuilder setOldMaxCount(int count) {
		mongoConfiguration.setOldMaxCount(count);
		return this;
	}
}