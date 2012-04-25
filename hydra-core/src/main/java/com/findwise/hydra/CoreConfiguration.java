package com.findwise.hydra;

public interface CoreConfiguration extends Configuration, DatabaseConfiguration {
	static final String NAMESPACE_PARAM = DatabaseConnector.NAMESPACE_PARAM;
	static final String DATABASE_URL_PARAM = DatabaseConnector.DATABASE_URL_PARAM;
	
	int getPollingInterval();
}
