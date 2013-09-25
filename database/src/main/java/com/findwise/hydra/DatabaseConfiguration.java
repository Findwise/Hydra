package com.findwise.hydra;

public interface DatabaseConfiguration {
	String DATABASE_URL_PARAM = "database.url";
	String DATABASE_USER = "database.username";
	String DATABASE_PASSWORD = "database.password";
	String DATABASE_NAMESPACE = "database.pipeline";
	String OLD_MAX_COUNT = "old.max_count";
	String OLD_MAX_SIZE_MB = "old.storage_size_mb";

	String getNamespace();

	String getDatabaseUrl();

	String getDatabaseUser();

	String getDatabasePassword();

	int getOldMaxSize();

	int getOldMaxCount();
}
