package com.findwise.hydra.admin.rest;

import org.springframework.beans.factory.annotation.Value;

import com.findwise.hydra.DatabaseConfiguration;

public class DatabaseConfig implements DatabaseConfiguration {

	private static final String DEFAULT_NAMESPACE = "pipeline";
	private static final String DEFAULT_DB_URL = "localhost";
	private static final String DEFAULT_DB_USER = "admin";
	private static final String DEFAULT_DB_PASSWORD = "changeme";
	private static final int DEFAULT_OLD_MAX_SIZE = 100;
	private static final int DEFAULT_OLD_MAX_COUNT = 10000;

	@Value("${admin.pipeline:pipeline}")
	private String namespace = DEFAULT_NAMESPACE;
	@Value("${database.url:localhost}")
	private String databaseUrl = DEFAULT_DB_URL;
	@Value("${database.username:admin}")
	private String databaseUser = DEFAULT_DB_USER;
	@Value("${database.password:changeme}")
	private String databasePassword = DEFAULT_DB_PASSWORD;
	@Value("${old.storage_size_mb:100}")
	private int oldMaxSize = DEFAULT_OLD_MAX_SIZE;
	@Value("${old.max_count:10000}")
	private int oldMaxCount = DEFAULT_OLD_MAX_COUNT;

	@Override
	public String getNamespace() {
		return namespace;
	}

	@Override
	public String getDatabaseUrl() {
		return databaseUrl;
	}

	@Override
	public String getDatabaseUser() {
		return databaseUser;
	}

	@Override
	public String getDatabasePassword() {
		return databasePassword;
	}

	@Override
	public int getOldMaxSize() {
		return oldMaxSize;
	}

	@Override
	public int getOldMaxCount() {
		return oldMaxCount;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public void setDatabaseUrl(String databaseUrl) {
		this.databaseUrl = databaseUrl;
	}

	public void setDatabaseUser(String databaseUser) {
		this.databaseUser = databaseUser;
	}

	public void setOldMaxSize(int oldMaxSize) {
		this.oldMaxSize = oldMaxSize;
	}

	public void setOldMaxCount(int oldMaxCount) {
		this.oldMaxCount = oldMaxCount;
	}

}
