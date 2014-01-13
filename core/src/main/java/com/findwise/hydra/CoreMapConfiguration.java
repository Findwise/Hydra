package com.findwise.hydra;

import com.findwise.hydra.local.RemotePipeline;
import com.findwise.hydra.mongodb.MongoConfiguration;

public class CoreMapConfiguration implements CoreConfiguration, Configuration {

	private final MongoConfiguration databaseConfiguration;
	private final MapConfiguration mapConfiguration;

	public CoreMapConfiguration(MongoConfiguration databaseConfiguration, MapConfiguration mapConfiguration) {
		this.databaseConfiguration = databaseConfiguration;
		this.mapConfiguration = mapConfiguration;
	}

	public void setNamespace(String value) {
		databaseConfiguration.setNamespace(value);
	}

	public String getNamespace() {
		return databaseConfiguration.getNamespace();
	}

	public void setDatabaseUrl(String value) {
		databaseConfiguration.setDatabaseUrl(value);
	}

	public String getDatabaseUrl() {
		return databaseConfiguration.getDatabaseUrl();
	}

	public void setPollingInterval(int value) {
		setParameter(PIPELINE_POLLING_INTERVAL, "" + value);
	}

	public int getPollingInterval() {
		return Integer.parseInt(getParameter(PIPELINE_POLLING_INTERVAL, ""
				+ NodeMaster.DEFAULT_POLLING_INTERVAL));
	}

	public void setRestPort(int value) {
		mapConfiguration.setParameter(COMMUNICATION_PORT_PARAM, "" + value);
	}

	public int getRestPort() {
		return Integer.parseInt(getParameter(COMMUNICATION_PORT_PARAM, ""
				+ RemotePipeline.DEFAULT_PORT));
	}

	public String getDatabaseUser() {
		return databaseConfiguration.getDatabaseUser();
	}

	public void setDatabaseUser(String user) {
		databaseConfiguration.setDatabaseUser(user);
	}

	public String getDatabasePassword() {
		return databaseConfiguration.getDatabasePassword();
	}

	public void setDatabasePassword(String password) {
		databaseConfiguration.setDatabasePassword(password);
	}

	public int getOldMaxSize() {
		return databaseConfiguration.getOldMaxSize();
	}

	public void setOldMaxSize(int size) {
		databaseConfiguration.setOldMaxSize(size);
	}

	public int getOldMaxCount() {
		return databaseConfiguration.getOldMaxCount();
	}

	public void setOldMaxCount(int count) {
		databaseConfiguration.setOldMaxCount(count);
	}

	public boolean isPerformanceLogging() {
		return Boolean.parseBoolean(getParameter(LOGGING_PERFORMANCE, "false"));
	}

	public void setCaching(boolean cache) {
		setParameter(USE_CACHE, "" + cache);
	}

	public boolean isCacheEnabled() {
		return Boolean.parseBoolean(getParameter(USE_CACHE, "false"));
	}

	public int getCacheTimeout() {
		return Integer.parseInt(getParameter(CACHE_TIMEOUT, ""
				+ CachingDocumentNIO.DEFAULT_CACHE_TIMEOUT));
	}
	
	public void setCacheTimeout(int timeout) {
		setParameter(CACHE_TIMEOUT, ""+timeout);
	}

    public int getLoggingPort() {
        return Integer.parseInt(getParameter(LOGGING_PORT, "" + DEFAULT_LOGGING_PORT));
    }

    public void setLoggingPort(int loggingPort) {
        setParameter(LOGGING_PORT, "" + loggingPort);
    }

	public String getParameter(String key) {
		return mapConfiguration.getParameter(key);
	}

	public String getParameter(String key, String defaultValue) {
		return mapConfiguration.getParameter(key, defaultValue);
	}

	public void setParameter(String key, String value) {
		mapConfiguration.setParameter(key, value);
	}
}
