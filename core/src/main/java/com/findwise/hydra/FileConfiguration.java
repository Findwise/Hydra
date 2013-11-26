package com.findwise.hydra;

import com.findwise.hydra.mongodb.MongoConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.local.RemotePipeline;

public class FileConfiguration implements CoreConfiguration, Configuration {
	public static final String DEFAULT_PROPERTIES_FILE = "resource.properties";
	
	private static Logger logger = LoggerFactory.getLogger(Configuration.class);
	
	private PropertiesConfiguration conf;
	
	public FileConfiguration() throws ConfigurationException {
		this(DEFAULT_PROPERTIES_FILE);
	}
	
	public FileConfiguration(String fileName) throws ConfigurationException {
		logger.debug("Properties: Reading from file '" + fileName + "'");
		conf = new PropertiesConfiguration(fileName);
	}
	
	public int getPollingInterval() {
		return conf.getInt(PIPELINE_POLLING_INTERVAL, NodeMaster.DEFAULT_POLLING_INTERVAL);
	}

	@Override
	public String getNamespace() {
		return conf.getString(DATABASE_NAMESPACE, MongoConfiguration.DATABASE_NAMESPACE_DEFAULT);
	}

	@Override
	public String getDatabaseUrl() {
		return conf.getString(DATABASE_URL_PARAM, MongoConfiguration.DATABASE_URL_PARAM_DEFAULT);
	}

	@Override
	public Object getParameter(String key) {
		return conf.getString(key);
	}

	@Override
	public Object getParameter(String key, String defaultValue) {
		return conf.getString(key, defaultValue);
	}

	@Override
	public int getRestPort() {
		return conf.getInt(COMMUNICATION_PORT_PARAM, RemotePipeline.DEFAULT_PORT);
	}

	@Override
	public String getDatabaseUser() {
		return conf.getString(DATABASE_USER, MongoConfiguration.DATABASE_USER_DEFAULT);
	}

	@Override
	public String getDatabasePassword() {
		return conf.getString(DATABASE_PASSWORD, MongoConfiguration.DATABASE_PASSWORD_DEFAULT);
	}

	@Override
	public int getOldMaxSize() {
		return conf.getInt(OLD_MAX_SIZE_MB, MongoConfiguration.OLD_MAX_SIZE_MB_DEFAULT);
	}

	@Override
	public int getOldMaxCount() {
		return conf.getInt(OLD_MAX_COUNT, MongoConfiguration.OLD_MAX_COUNT_DEFAULT);
	}
	
	@Override
	public boolean isPerformanceLogging() {
		return conf.getBoolean(LOGGING_PERFORMANCE, false);
	}

	@Override
	public boolean isCacheEnabled() {
		return conf.getBoolean(USE_CACHE, false);
	}

	@Override
	public int getCacheTimeout() {
		return conf.getInt(CACHE_TIMEOUT, CachingDocumentNIO.DEFAULT_CACHE_TIMEOUT);
	}

    @Override
    public int getLoggingPort() {
        return conf.getInt(LOGGING_PORT, DEFAULT_LOGGING_PORT);
    }
}
