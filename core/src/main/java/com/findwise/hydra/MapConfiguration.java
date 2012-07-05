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
		map.put(DatabaseConnector.NAMESPACE_PARAM, value);
	}
	
	@Override
	public String getNamespace() {
		return getParameter(DatabaseConnector.NAMESPACE_PARAM);
	}

	public void setDatabaseUrl(String value) {
		map.put(DatabaseConnector.DATABASE_URL_PARAM, value);
	}
	
	@Override
	public String getDatabaseUrl() {
		return getParameter(DatabaseConnector.DATABASE_URL_PARAM, DATABASE_URL_DEFAULT);
	}

	public void setPollingInterval(int value) {
		map.put(POLLING_INTERVAL_PARAM, ""+value);
	}
	
	@Override
	public int getPollingInterval() {
		return Integer.parseInt(getParameter(POLLING_INTERVAL_PARAM, ""+POLLING_INTERVAL_DEFAULT));
	}

	public void setRestPort(int value) {
		map.put(REST_PORT_PARAM, ""+value);
	}

	@Override
	public int getRestPort() {
		return Integer.parseInt(getParameter(REST_PORT_PARAM, ""+RemotePipeline.DEFAULT_PORT));
	}

	@Override
	public String getParameter(String key) {
		if(map.containsKey(key)) {
			return map.get(key);
		}
		throw new NoSuchElementException("No setting found for key: "+key);
	}
	
	@Override
	public String getParameter(String key, String defaultValue) {
		if(map.containsKey(key)) {
			return map.get(key);
		}
		return defaultValue;
	}

	@Override
	public String getDatabaseUser() {
		return getParameter(DatabaseConnector.DATABASE_USER, "admin");
	}

	public void setDatabaseUser(String user) {
		map.put(DatabaseConnector.DATABASE_USER, user);
	}

	@Override
	public String getDatabasePassword() {
		return getParameter(DatabaseConnector.DATABASE_PASSWORD, "changeme");
	}

	public void setDatabasePassword(String password) {
		map.put(DatabaseConnector.DATABASE_PASSWORD, password);
	}
}
