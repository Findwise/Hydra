package com.findwise.hydra.mongodb;

import com.findwise.hydra.DatabaseConfiguration;

public class DatabaseConfigurationFactory {
	public static DatabaseConfiguration getDatabaseConfiguration(final String namespace) {
		return new DatabaseConfiguration() {
			
			@Override
			public int getOldMaxSize() {
				return 10;
			}
			
			@Override
			public int getOldMaxCount() {
				return 10;
			}
			
			@Override
			public String getNamespace() {
				return namespace;
			}
			
			@Override
			public String getDatabaseUser() {
				return "test";
			}
			
			@Override
			public String getDatabaseUrl() {
				return "localhost";
			}
			
			@Override
			public String getDatabasePassword() {
				return "test";
			}
		};
	}
}
