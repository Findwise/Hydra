package com.findwise.hydra;


/**
 * Provides connection details to a mongodb instance.
 *
 * @author johan.sjoberg
 */
public class MongoDBConnectionConfig {

	private final String namespace;
	private final String connectionUrl;
	private final int connectionPort;
	private final String username;
	private final String password;
	private final int OLD_MAX_SIZE = 100;
	private final int OLD_MAX_NUMBER = 1000;

	/**
	 * Creates a new module with the given namespace and connection string.
	 *
	 * @param namespace
	 * 		mongodb namespace
	 * @param connectionUrl
	 * 		mongodb connection url, e.g., "127.0.0.1"
	 */
	public MongoDBConnectionConfig(String namespace, String connectionUrl) {
		this(namespace, connectionUrl, 27017, null, null);
	}

	/**
	 * Creates a new module with with the given namespace, connection string, username and
	 * password
	 *
	 * @param namespace
	 * 		mongodb namespace
	 * @param connectionUrl
	 * 		mongodb host, e.g., "127.0.0.1"
	 * @param connectionPort
	 * 		mongodb port
	 * @param username
	 * 		mongodb username
	 * @param password
	 * 		mongodb password
	 */
	public MongoDBConnectionConfig(String namespace,
			String connectionUrl,
			int connectionPort,
			String username,
			String password) {
		this.namespace = namespace;
		this.connectionUrl = connectionUrl;
		this.connectionPort = connectionPort;
		this.username = username;
		this.password = password;
	}

	public DatabaseConfiguration getConfiguration() {
		return new DatabaseConfiguration() {

			@Override
			public String getNamespace() {
				return namespace;
			}

			@Override
			public String getDatabaseHost() {
				return connectionUrl;
			}

			@Override
			public int getDatabasePort() {
				return connectionPort;
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
				return OLD_MAX_SIZE;
			}

			@Override
			public int getOldMaxCount() {
				return OLD_MAX_NUMBER;
			}
		};
	}
}
