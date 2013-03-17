package com.findwise.hydra;

public interface CoreConfiguration extends Configuration, DatabaseConfiguration {
	static final String PERFORMANCE_LOGGING = "performance_logging";
	static final String USE_CACHE = "cache";
	
	int getPollingInterval();
	
	boolean isPerformanceLogging();
	
	boolean isCaching();
}
