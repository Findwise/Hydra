package com.findwise.hydra;

public interface CoreConfiguration extends Configuration, DatabaseConfiguration {
	static final String PERFORMANCE_LOGGING = "performance_logging";
	static final String LOGGING_PORT = "logging_port";
    static final int DEFAULT_LOGGING_PORT = 12002;

    int getPollingInterval();
	
	boolean isPerformanceLogging();

    int getLoggingPort();
}
