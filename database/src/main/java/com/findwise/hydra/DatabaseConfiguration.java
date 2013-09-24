package com.findwise.hydra;

public interface DatabaseConfiguration {
	String DATABASE_URL_PARAM = "database.host";
	String DATABASE_USER = "database.username";
	String DATABASE_PASSWORD = "database.password";
	String OLD_MAX_COUNT = "old.max_count";
	String OLD_MAX_SIZE_MB = "old.storage_size_mb";
	
	String DATABASE_URL_DEFAULT = "mongodb://127.0.0.1";

	String getNamespace();

	String getDatabaseUrl();
	
	String getDatabaseUser();
	
	String getDatabasePassword();
	
	int getOldMaxSize();
	
	int getOldMaxCount();
}
