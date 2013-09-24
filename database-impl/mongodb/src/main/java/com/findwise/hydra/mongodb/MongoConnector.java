package com.findwise.hydra.mongodb;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

import com.mongodb.MongoClientURI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.DatabaseConfiguration;
import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.Document;
import com.findwise.hydra.JsonException;
import com.findwise.hydra.PipelineReader;
import com.findwise.hydra.Query;
import com.findwise.hydra.StatusUpdater;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.LocalQuery;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.gridfs.GridFS;

public class MongoConnector implements DatabaseConnector<MongoType> {
	public static final int OLD_DOCUMENTS_TO_KEEP_DEFAULT = 1000;

	protected static final String TMP_DIR = "tmp";

	private DB db;

	private MongoStatusIO statusIO;

	private StatusUpdater statusUpdater;

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

	private final Logger logger = LoggerFactory.getLogger(MongoConnector.class);

	private WriteConcern concern = WriteConcern.NORMAL;

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

	private final DatabaseConfiguration conf;

	private MongoPipelineReader pipelineReader;
	private MongoPipelineWriter pipelineWriter;
	private MongoDocumentIO documentIO;

	private boolean connected = false;

	public MongoConnector(DatabaseConfiguration conf) {
		this.conf = conf;
	}

	@Override
	public void connect() throws IOException {
		MongoClient mongo;
		try {
			mongo = new MongoClient(new MongoClientURI(conf.getDatabaseUrl()));
		} catch (UnknownHostException e) {
			logger.error("Failed to establish connection to MongoDB at URL: "
					+ conf.getDatabaseUrl(), e);
			throw new ConnectionException(e);

		} catch (MongoException e) {
			logger.error("A MongoException occurred", e);
			throw new ConnectionException(e);
		}
		connect(mongo, true);
	}

	protected void connect(MongoClient mongo, boolean startStatusUpdater) throws IOException {
		db = mongo.getDB(conf.getNamespace());

		if (requiresAuthentication(mongo)) {
			if (!mongo.getDB("admin").authenticate(conf.getDatabaseUser(),
					conf.getDatabasePassword().toCharArray())) {
				throw new ConnectionException(new MongoException(
						"Failed to authenticate to MongoDB with username="
								+ conf.getDatabaseUser()));
			}
		}

		pipelineReader = new MongoPipelineReader(db);
		pipelineWriter = new MongoPipelineWriter(pipelineReader, concern);

		statusIO = new MongoStatusIO(db);

		MongoPipelineStatus pipelineStatus;
		if (!statusIO.hasStatus()) {
			pipelineStatus = new MongoPipelineStatus();

			statusIO.save(pipelineStatus);
		} else {
			pipelineStatus = statusIO.getStatus();
		}

		statusUpdater = new StatusUpdater(this);

		if (!pipelineStatus.isPrepared()) {
			logger.info("Database is new, preparing it");
			pipelineStatus.setPrepared(true);
			pipelineStatus.setDiscardedMaxSize(conf.getOldMaxSize());
			pipelineStatus.setDiscardedToKeep(conf.getOldMaxCount());

			documentIO = new MongoDocumentIO(db, concern,
					pipelineStatus.getNumberToKeep(),
					pipelineStatus.getDiscardedMaxSize(), statusUpdater, new GridFS(db,
							MongoDocumentIO.DOCUMENT_FS));
			documentIO.prepare();
			pipelineWriter.prepare();

			statusIO.save(pipelineStatus);
		} else {
			documentIO = new MongoDocumentIO(db, concern,
					pipelineStatus.getNumberToKeep(),
					pipelineStatus.getDiscardedMaxSize(), statusUpdater, new GridFS(db,
							MongoDocumentIO.DOCUMENT_FS));
		}

		connected = true;

		if (startStatusUpdater) {
			statusUpdater.start();
		}
	}

	public StatusUpdater getStatusUpdater() {
		return statusUpdater;
	}

	private boolean requiresAuthentication(MongoClient mongo) {
		try {
			mongo.getDatabaseNames();
			return false;
		} catch (MongoException e) {
			return true;
		}
	}

	@Override
	public boolean isConnected() {
		return connected;
	}

	@Override
	public MongoQuery convert(Query query) {
		try {
			return new MongoQuery(query.toJson());
		} catch (JsonException e) {
			return null;
		}
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
	public PipelineReader getPipelineReader() {
		return pipelineReader;
	}

	public DB getDB() {
		return db;
	}

	@Override
	public MongoStatusIO getStatusWriter() {
		return statusIO;
	}

	@Override
	public MongoStatusIO getStatusReader() {
		return statusIO;
	}

	@Override
	public DatabaseDocument<MongoType> convert(Document<?> document)
			throws ConversionException {
		try {
			if (contains(document, "\u0000")) {
				throw new ConversionException(
						"A document cannot contain the NUL character. See https://jira.mongodb.org/browse/SERVER-7691");
			}
			return new MongoDocument(document.toJson());
		} catch (JsonException e) {
			throw new ConversionException(
					"JSON Exception caught while converting", e);
		}
	}

	private boolean contains(Document<?> haystack, String needle) {
		for (String field : haystack.getContentFields()) {
			if (field.contains(needle)) {
				return true;
			}
			if (contains(haystack.getContentField(field), needle)) {
				return true;
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	private boolean contains(Object object, String needle) {
		if (object instanceof String) {
			return ((String) object).contains(needle);
		} else if (object instanceof List<?>) {
			for (Object o : (List<?>) object) {
				if (contains(o, needle)) {
					return true;
				}
			}
		} else if (object instanceof Map<?, ?>) {
			for (Map.Entry<Object, Object> o : ((Map<Object, Object>) object)
					.entrySet()) {
				if (contains(o.getKey(), needle)
						|| contains(o.getValue(), needle)) {
					return true;
				}
			}
		}
		return false;
	}
}
