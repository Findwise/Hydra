package com.findwise.hydra;

public interface CoreConfiguration extends Configuration, DatabaseConfiguration {
	static final String PERFORMANCE_LOGGING = "performance_logging";
	
	int getPollingInterval();
	
	boolean isPerformanceLogging();
}
