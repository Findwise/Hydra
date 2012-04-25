package com.findwise.hydra;

public interface DatabaseConfiguration {
	String DATABASE_URL_DEFAULT = "127.0.0.1";

	String getNamespace();

	String getDatabaseUrl();
}
