package com.findwise.hydra.debugging;

import com.findwise.hydra.DatabaseConfiguration;
import com.findwise.hydra.JsonException;
import com.findwise.hydra.StatusUpdater;
import com.findwise.hydra.mongodb.MongoConfiguration;
import com.findwise.hydra.mongodb.MongoConnector;
import com.findwise.hydra.mongodb.MongoDocument;
import com.findwise.hydra.mongodb.MongoDocumentIO;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteConcern;
import com.mongodb.gridfs.GridFS;

import java.io.IOException;

public class StdinInput {

	private static DatabaseConfiguration conf = new MongoConfiguration();

	public static void main(String[] args) throws IOException, JsonException {
		MongoClient mongo = new MongoClient(new MongoClientURI(conf.getDatabaseUrl()));
		DB db = mongo.getDB(conf.getNamespace());
		WriteConcern concern = mongo.getWriteConcern();
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
