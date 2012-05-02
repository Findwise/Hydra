package com.findwise.hydra.mongodb;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.PipelineReader;
import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.LocalQuery;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

public class MongoConnector implements DatabaseConnector<MongoType> {

	public static final String HYDRA_COLLECTION_NAME = "hydra";
	public static final int OLD_DOCUMENTS_TO_KEEP_DEFAULT = 1000;
	
	private DBCollection hydraCollection;
	
	protected static final String TMP_DIR = "tmp";
	
	private DB db;

	/**
	 * Creates the tmp dir if it doesn't exist
	 */
	protected static void createTmpDir() throws IOException {
		File tmpdir = new File(TMP_DIR);
		if (!tmpdir.exists()) {
			if (!tmpdir.mkdir()) {
				throw new IOException("Unable to create tmp directory");
			}
		}
	}

	private Logger logger = LoggerFactory.getLogger(MongoConnector.class);

	private WriteConcern concern = WriteConcern.NORMAL;

	public String getUrl() {
		return url;
	}

	public String getPipelineName() {
		return pipelineName;
	}

	@Override
	public MongoPipelineWriter getPipelineWriter() {
		return pipelineWriter;
	}

	@Override
	public MongoDocumentIO getDocumentReader() {
		return documentIO;
	}

	@Override
	public MongoDocumentIO getDocumentWriter() {
		return documentIO;
	}

	private String url;
	private String pipelineName;

	private MongoPipelineReader pipelineReader;
	private MongoPipelineWriter pipelineWriter;
	private MongoDocumentIO documentIO;
	
	private boolean connected = false;

	@Inject
	public MongoConnector(@Named(DATABASE_URL_PARAM) String url,
			@Named(NAMESPACE_PARAM) String namespace) {
		this.url = url;
		this.pipelineName = namespace;
	}

	@Override
	public void connect() throws IOException {
		Mongo mongo;
		try {
			mongo = new Mongo(url);
		} catch (UnknownHostException e) {
			logger.error("Failed to establish connection to MongoDB at URL: "
					+ url, e);
			throw new ConnectionException(e);

		} catch (MongoException e) {
			logger.error("A MongoException occurred", e);
			throw new ConnectionException(e);
		}
		db = mongo.getDB(pipelineName);


		pipelineReader = new MongoPipelineReader(db);
		pipelineWriter = new MongoPipelineWriter(pipelineReader, concern);

		hydraCollection = db.getCollection(HYDRA_COLLECTION_NAME);
		hydraCollection.setObjectClass(MongoPipelineStatus.class);
		
		if(hydraCollection.count()==0) {
			MongoPipelineStatus conf = getNewPipelineStatus();
			//TODO: Default behavior is to discard
			conf.setDiscardOldDocuments(true);
			hydraCollection.insert(conf);
		}
		
		MongoPipelineStatus pipelineStatus = getPipelineStatus();
		
		documentIO = new MongoDocumentIO(db, concern, pipelineStatus.isDiscardingOldDocuments(), pipelineStatus.getNumberToKeep());
	
		if(!pipelineStatus.isPrepared()) {
			logger.info("Database is new, preparing it");
			documentIO.prepare();
			pipelineWriter.prepare();
			pipelineStatus.setPrepared(true);
			hydraCollection.save(pipelineStatus);
		}

		connected = true;
	}
	
	@Override
	public MongoPipelineStatus getNewPipelineStatus() {
		return new MongoPipelineStatus();
	}
	
	@Override
	public MongoPipelineStatus getPipelineStatus() {
		return (MongoPipelineStatus) hydraCollection.findOne();
	}
	
	@Override
	public boolean isConnected() {
		return connected;
	}

	@Override
	public MongoQuery convert(LocalQuery query) {
		return staticConvert(query);
	}

	@Override
	public MongoDocument convert(LocalDocument document) {
		return staticConvert(document);
	}

	public static MongoDocument staticConvert(LocalDocument document) {
		MongoDocument doc;
		try {
			doc = new MongoDocument(document.toJson());
			return doc;
		} catch (JsonException e) {
			return null;
		}
	}

	public static MongoQuery staticConvert(LocalQuery query) {
		MongoQuery mdq;
		try {
			mdq = new MongoQuery(query.toJson());
			return mdq;
		} catch (JsonException e) {
			return null;
		}
	}

	@SuppressWarnings("serial")
	private static class ConnectionException extends IOException {
		public ConnectionException(Throwable t) {
			super(t);
		}
	}

	@Override
	public void waitForWrites(boolean alwaysWait) {
		if (alwaysWait) {
			concern = WriteConcern.SAFE;
		} else {
			concern = WriteConcern.NORMAL;
		}
	}

	@Override
	public boolean isWaitingForWrites() {
		return WriteConcern.SAFE == concern;
	}

	@Override
	public PipelineReader<MongoType> getPipelineReader() {
		return pipelineReader;
	}
	
	
	public DB getDB() {
		return db;
	}
}
