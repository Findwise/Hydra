package com.findwise.hydra;

public interface DatabaseConfiguration {
	String DATABASE_HOST = "database.host";
	String DATABASE_PORT = "database.port";
	String DATABASE_USER = "database.username";
	String DATABASE_PASSWORD = "database.password";
	String OLD_MAX_COUNT = "old.max_count";
	String OLD_MAX_SIZE_MB = "old.storage_size_mb";

	String DATABASE_HOST_DEFAULT = "127.0.0.1";
	int DATABASE_PORT_DEFAULT = 27017;

	String getNamespace();

	String getDatabaseHost();

	int getDatabasePort();

	String getDatabaseUser();

	String getDatabasePassword();

	int getOldMaxSize();

	int getOldMaxCount();
}
