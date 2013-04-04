package com.findwise.hydra;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import com.findwise.hydra.local.RemotePipeline;

public class MapConfiguration implements CoreConfiguration {
	private Map<String, String> map;

	public MapConfiguration() {
		map = new HashMap<String, String>();
	}

	public void setNamespace(String value) {
		map.put(NAMESPACE_PARAM, value);
	}

	@Override
	public String getNamespace() {
		return getParameter(NAMESPACE_PARAM);
	}

	public void setDatabaseUrl(String value) {
		map.put(DATABASE_URL_PARAM, value);
	}

	@Override
	public String getDatabaseUrl() {
		return getParameter(DATABASE_URL_PARAM,
				DATABASE_URL_DEFAULT);
	}

	public void setPollingInterval(int value) {
		map.put(PIPELINE_POLLING_INTERVAL, "" + value);
	}

	@Override
	public int getPollingInterval() {
		return Integer.parseInt(getParameter(PIPELINE_POLLING_INTERVAL, ""
				+ NodeMaster.DEFAULT_POLLING_INTERVAL));
	}

	public void setRestPort(int value) {
		map.put(COMMUNICATION_PORT_PARAM, "" + value);
	}

	@Override
	public int getRestPort() {
		return Integer.parseInt(getParameter(COMMUNICATION_PORT_PARAM, ""
				+ RemotePipeline.DEFAULT_PORT));
	}

	@Override
	public String getParameter(String key) {
		if (map.containsKey(key)) {
			return map.get(key);
		}
		throw new NoSuchElementException("No setting found for key: " + key);
	}

	@Override
	public String getParameter(String key, String defaultValue) {
		if (map.containsKey(key)) {
			return map.get(key);
		}
		return defaultValue;
	}

	@Override
	public String getDatabaseUser() {
		return getParameter(DATABASE_USER, "admin");
	}

	public void setDatabaseUser(String user) {
		map.put(DATABASE_USER, user);
	}

	@Override
	public String getDatabasePassword() {
		return getParameter(DATABASE_PASSWORD, "changeme");
	}

	public void setDatabasePassword(String password) {
		map.put(DATABASE_PASSWORD, password);
	}

	@Override
	public int getOldMaxSize() {
		return Integer
				.parseInt(getParameter(OLD_MAX_SIZE_MB));
	}

	public void setOldMaxSize(int size) {
		map.put(OLD_MAX_SIZE_MB, "" + size);
	}

	@Override
	public int getOldMaxCount() {
		return Integer.parseInt(getParameter(OLD_MAX_COUNT));
	}

	public void setOldMaxCount(int count) {
		map.put(OLD_MAX_COUNT, "" + count);
	}

	@Override
	public boolean isPerformanceLogging() {
		return Boolean.parseBoolean(getParameter(LOGGING_PERFORMANCE, "false"));
	}

	public void setCaching(boolean cache) {
		map.put(USE_CACHE, "" + cache);
	}

	@Override
	public boolean isCacheEnabled() {
		return Boolean.parseBoolean(getParameter(USE_CACHE, "false"));
	}

	@Override
	public int getCacheTimeout() {
		return Integer.parseInt(getParameter(getParameter(CACHE_TIMEOUT, ""
				+ CachingDocumentNIO.CACHED_TIME_METADATA_KEY)));
	}
	
	public void setCacheTimeout(int timeout) {
		map.put(CACHE_TIMEOUT, ""+timeout);
	}

    @Override
    public int getLoggingPort() {
        return Integer.parseInt(getParameter(LOGGING_PORT, "" + DEFAULT_LOGGING_PORT));
    }

    public void setLoggingPort(int loggingPort) {
        map.put(LOGGING_PORT, "" + loggingPort);
    }
}
