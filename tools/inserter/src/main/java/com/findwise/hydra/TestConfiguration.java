package com.findwise.hydra;

public class TestConfiguration implements DatabaseConfiguration {
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

	public TestConfiguration() {
		this("pipeline", "127.0.0.1");
	}

	public TestConfiguration(String namespace, String mongohost) {
		this(namespace, mongohost, "admin", "changeme");
	}

	public TestConfiguration(String namespace, String mongohost,
							 String username, String password) {
		this.namespace = namespace;
		this.mongohost = mongohost;
		this.username = username;
		this.password = password;
	}

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
}
