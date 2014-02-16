package com.findwise.hydra.admin.rest;

import com.findwise.hydra.mongodb.MongoConfiguration;
import org.springframework.beans.factory.annotation.Value;

import com.findwise.hydra.DatabaseConfiguration;

public class DatabaseConfig implements DatabaseConfiguration {

	@Value("${admin.pipeline:pipeline}")
	private String namespace = MongoConfiguration.DATABASE_NAMESPACE_DEFAULT;
	@Value("${database.url:\"mongodb://localhost\"}")
	private String databaseUrl = MongoConfiguration.DATABASE_URL_PARAM_DEFAULT;
	@Value("${database.username:admin}")
	private String databaseUser = MongoConfiguration.DATABASE_USER_DEFAULT;
	@Value("${database.password:changeme}")
	private String databasePassword = MongoConfiguration.DATABASE_PASSWORD_DEFAULT;
	@Value("${old.storage_size_mb:200}")
	private int oldMaxSize = MongoConfiguration.OLD_MAX_SIZE_MB_DEFAULT;
	@Value("${old.max_count:2000}")
	private int oldMaxCount = MongoConfiguration.OLD_MAX_SIZE_MB_DEFAULT;

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
