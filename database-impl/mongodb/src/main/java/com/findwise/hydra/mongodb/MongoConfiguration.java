package com.findwise.hydra.mongodb;

import com.findwise.hydra.Configuration;
import com.findwise.hydra.DatabaseConfiguration;
import com.findwise.hydra.MapConfiguration;

public class MongoConfiguration implements Configuration, DatabaseConfiguration {

	public static final String DATABASE_URL_PARAM_DEFAULT = "mongodb://localhost:27017";
	public static final String DATABASE_NAMESPACE_DEFAULT = "pipeline";
	public static final String DATABASE_USER_DEFAULT = "admin";
	public static final String DATABASE_PASSWORD_DEFAULT = "changeme";
	public static final int OLD_MAX_SIZE_MB_DEFAULT = 200;
	public static final int OLD_MAX_COUNT_DEFAULT = 2000;

	private final MapConfiguration mapConfiguration;

	public MongoConfiguration() {
		this.mapConfiguration = new MapConfiguration();
	}

	public MongoConfiguration(MapConfiguration mapConfiguration) {
		this.mapConfiguration = mapConfiguration;
	}

	public String getNamespace() {
		return getParameter(DATABASE_NAMESPACE, DATABASE_NAMESPACE_DEFAULT);
	}

	public void setNamespace(String value) {
		setParameter(DATABASE_NAMESPACE, value);
	}

	@Override
	public String getDatabaseUrl() {
		return getParameter(DATABASE_URL_PARAM, DATABASE_URL_PARAM_DEFAULT);
	}

	public void setDatabaseUrl(String value) {
		setParameter(DATABASE_URL_PARAM, value);
	}

	public String getDatabaseUser() {
		return getParameter(DATABASE_USER, DATABASE_USER_DEFAULT);
	}

	public void setDatabaseUser(String user) {
		setParameter(DATABASE_USER, user);
	}

	public String getDatabasePassword() {
		return getParameter(DATABASE_PASSWORD, DATABASE_PASSWORD_DEFAULT);
	}

	public void setDatabasePassword(String password) {
		setParameter(DATABASE_PASSWORD, password);
	}

	public int getOldMaxSize() {
		return Integer.parseInt(getParameter(OLD_MAX_SIZE_MB, "" + OLD_MAX_SIZE_MB_DEFAULT));
	}

	public void setOldMaxSize(int size) {
		setParameter(OLD_MAX_SIZE_MB, "" + size);
	}

	public int getOldMaxCount() {
		return Integer.parseInt(getParameter(OLD_MAX_COUNT, "" + OLD_MAX_COUNT_DEFAULT));
	}

	public void setOldMaxCount(int count) {
		setParameter(OLD_MAX_COUNT, "" + count);
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
