package com.findwise.hydra;

public interface CoreConfiguration extends DatabaseConfiguration {
	static final String LOGGING_PERFORMANCE = "core.logging.performance";
	static final String LOGGING_PORT = "core.logging.port";
    static final int DEFAULT_LOGGING_PORT = 12002;
	static final String USE_CACHE = "core.cache.enabled";
	static final String CACHE_TIMEOUT = "core.cache.timeout";
	static final String PIPELINE_POLLING_INTERVAL = "core.polling_interval";
	static final String COMMUNICATION_PORT_PARAM = "core.communication_port";
	
	int getRestPort();

	int getPollingInterval();
	
	boolean isPerformanceLogging();
	
	boolean isCacheEnabled();
	
	int getCacheTimeout();

    int getLoggingPort();
}
