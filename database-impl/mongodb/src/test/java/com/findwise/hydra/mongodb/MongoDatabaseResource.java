package com.findwise.hydra.mongodb;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import org.junit.rules.ExternalResource;

public class MongoDatabaseResource extends ExternalResource {

	public static final String DB_PREFIX = "junit-";

	private final String dbName;

	private MongoClient mongo;

	private DB db;

	public MongoDatabaseResource(Class<?> testClass) {
		this.dbName = DB_PREFIX + testClass.getSimpleName();
	}

	@Override
	protected void before() throws Throwable {
		mongo = new MongoClient();
		db = mongo.getDB(dbName);
	}

	@Override
	protected void after() {
		mongo.dropDatabase(dbName);
		mongo.close();
	}
	
	public DB getDatabase() {
		return db;
	}
}
