package com.findwise.hydra;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.local.RemotePipeline;

public class FileConfiguration implements CoreConfiguration {
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
		return conf.getInt(POLLING_INTERVAL_PARAM, POLLING_INTERVAL_DEFAULT);
	}

	@Override
	public String getNamespace() {
		return conf.getString(DatabaseConnector.NAMESPACE_PARAM);
	}

	@Override
	public String getDatabaseUrl() {
		return conf.getString(DatabaseConnector.DATABASE_URL_PARAM, DATABASE_URL_DEFAULT);
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
		return conf.getInt(REST_PORT_PARAM, RemotePipeline.DEFAULT_PORT);
	}

	@Override
	public String getDatabaseUser() {
		return conf.getString(DatabaseConnector.DATABASE_USER, "");
	}

	@Override
	public String getDatabasePassword() {
		return conf.getString(DatabaseConnector.DATABASE_PASSWORD, "");
	}

	@Override
	public int getOldMaxSize() {
		return conf.getInt(DatabaseConnector.OLD_MAX_SIZE_MB, 100);
	}

	@Override
	public int getOldMaxCount() {
		return conf.getInt(DatabaseConnector.OLD_MAX_COUNT, 1000);
	}
	
	
}
