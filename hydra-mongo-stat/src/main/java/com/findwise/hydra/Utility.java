package com.findwise.hydra;

import java.io.IOException;

import com.findwise.hydra.mongodb.MongoConnector;

public class Utility {

	private static DatabaseConnector<?> instance = null;

	public static DatabaseConnector<?> getConnectorInstance()
			throws IOException {
		if (instance == null) {
			instance = new MongoConnector(new DatabaseConfiguration() {

				public int getOldMaxSize() {
					return 100;
				}

				public int getOldMaxCount() {
					return 2000;
				}

				public String getNamespace() {
					return "pipeline";
				}

				public String getDatabaseUser() {
					// TODO Auto-generated method stub
					return null;
				}

				public String getDatabaseUrl() {
					return "localhost";
				}

				public String getDatabasePassword() {
					// TODO Auto-generated method stub
					return null;
				}
			});
			instance.connect();
		}

		return instance;
	}
}
