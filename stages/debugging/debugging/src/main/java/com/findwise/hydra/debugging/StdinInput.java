package com.findwise.hydra.debugging;

import java.io.IOException;

import com.findwise.hydra.DatabaseConfiguration;
import com.findwise.hydra.JsonException;
import com.findwise.hydra.StatusUpdater;
import com.findwise.hydra.mongodb.MongoConnector;
import com.findwise.hydra.mongodb.MongoDocument;
import com.findwise.hydra.mongodb.MongoDocumentIO;
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;
import com.mongodb.gridfs.GridFS;

public class StdinInput {

	private static DatabaseConfiguration conf = new DatabaseConfiguration() {

		public int getOldMaxSize() {
			return 200;
		}

		public int getOldMaxCount() {
			return 2000;
		}

		public String getNamespace() {
			return "pipeline";
		}

		public String getDatabaseUrl() {
			return "localhost";
		}

		public String getDatabaseUser() {
			return null;
		}

		public String getDatabasePassword() {
			return null;
		}
	};

	public static void main(String[] args) throws IOException, JsonException {
		Mongo mongo = new Mongo(conf.getDatabaseUrl());
		DB db = mongo.getDB(conf.getNamespace());
		WriteConcern concern = new WriteConcern();
		long documentsToKeep = conf.getOldMaxCount();
		int oldDocsMaxSizeMB = conf.getOldMaxSize();
		StatusUpdater updater = new StatusUpdater(new MongoConnector(conf));
		GridFS documentFs = new GridFS(db);

		MongoDocumentIO io = new MongoDocumentIO(db, concern, documentsToKeep,
				oldDocsMaxSizeMB, updater, documentFs);
		io.prepare();

		MongoDocument document = new MongoDocument(args[args.length-1]);
		io.insert(document);
		System.out.println("Added document");
	}

}
