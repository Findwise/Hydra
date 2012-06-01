package com.findwise.hydra.mongodb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.DatabaseQuery;
import com.findwise.hydra.DocumentReader;
import com.findwise.hydra.DocumentWriter;
import com.findwise.hydra.common.Document;
import com.findwise.hydra.common.DocumentFile;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.QueryBuilder;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

/**
 * @author joel.westberg
 *
 */
public class MongoDocumentIO implements DocumentReader<MongoType>, DocumentWriter<MongoType> {
	private DBCollection documents;
	private DBCollection oldDocuments;
	private GridFS documentfs;
	private WriteConcern concern;
	
	private HashSet<String> seenTags = new HashSet<String>();
	
	public static final int OLD_DOCUMENT_AVG_SIZE = 10000;
	
	private static Logger logger = LoggerFactory.getLogger(MongoDocumentIO.class);

	protected static final String FETCHED_TAG = "fetched";
	protected static final String TOUCHED_TAG = "touched";
	
	private long maxDocumentsToKeep;
	
	public static final String DOCUMENT_COLLECTION = "documents";
	public static final String OLD_DOCUMENT_COLLECTION ="oldDocuments";
	public static final String DOCUMENT_FS = "documents";

	public static final int DEFAULT_RECURRING_INTERVAL = 2000;
	
	public static final String DOCUMENT_KEY = "document";
	
	private boolean discardOld = false;
	
	public MongoDocumentIO(DB db, WriteConcern concern, boolean discard, long documentsToKeep) {
		this.concern = concern;
		this.discardOld = discard;
		this.maxDocumentsToKeep = documentsToKeep;
		
		documents = db.getCollection(DOCUMENT_COLLECTION);
		documents.setObjectClass(MongoDocument.class);
		oldDocuments = db.getCollection(OLD_DOCUMENT_COLLECTION);
		oldDocuments.setObjectClass(MongoDocument.class);
		documentfs = new GridFS(db, DOCUMENT_FS);
	}
	
	@Override
	public void setDiscardOld(boolean discard) {
		discardOld = discard;
	}
	
	@Override
	public void prepare() {
		if(discardOld) {
			capIfNew(documents.getDB(), maxDocumentsToKeep*OLD_DOCUMENT_AVG_SIZE, maxDocumentsToKeep);
		}
		oldDocuments = documents.getDB().getCollection(OLD_DOCUMENT_COLLECTION);
		oldDocuments.setObjectClass(MongoDocument.class);
	}
	
	private void capIfNew(DB db, long size, long max) {
		if(!db.getCollectionNames().contains(OLD_DOCUMENT_COLLECTION)) {
			BasicDBObject dbo = new BasicDBObject("create", OLD_DOCUMENT_COLLECTION);
			dbo.put("capped", true);
			dbo.put("size", size);
			dbo.put("max", max);
			CommandResult cr = db.command(dbo);
			if(cr.ok()) {
				logger.info("Created a capped collection for old documents with {size: "+size+", max: "+max+"}");
			}
			else {
				if(db.getCollectionNames().contains(OLD_DOCUMENT_COLLECTION)) {
					logger.debug("Raced to create "+OLD_DOCUMENT_COLLECTION+" collection and lost");
				}
				else {
					logger.error("Unable to create capped collection for old documents, result was: "+cr);
				}
			}
		}
	}
	
	
	/* (non-Javadoc)
	 * @see com.findwise.hydra.DocumentReader#getDocumentFile(com.findwise.hydra.DatabaseDocument)
	 */
	@Override
	public DocumentFile getDocumentFile(DatabaseDocument<MongoType> d) throws IOException {
		MongoDocument md = (MongoDocument)d;
		DBObject query = QueryBuilder.start(DOCUMENT_KEY).is(md.getID()).get();
		GridFSDBFile file = documentfs.findOne(query);
		if(file==null) {
			return null;
		}
		
		return new DocumentFile(d.getID(), file.getFilename(), file.getInputStream(), file.getUploadDate());
	}
	
	
	/* (non-Javadoc)
	 * @see com.findwise.hydra.DocumentReader#getDocument(com.findwise.hydra.DatabaseQuery)
	 */
	@Override
	public MongoDocument getDocument(DatabaseQuery<MongoType> dbq) {
		DBObject query = ((MongoQuery)dbq).toDBObject();
		MongoDocument doc = (MongoDocument) documents.findOne(query);
		if (doc==null) {
			return null;
		}
		return doc;
	}
	
	/* (non-Javadoc)
	 * @see com.findwise.hydra.DocumentReader#getDocumentById(java.lang.Object)
	 */
	@Override
	public MongoDocument getDocumentById(Object id) {
		MongoQuery mq = new MongoQuery();
		mq.requireID(id);
		MongoDocument doc = (MongoDocument) documents.findOne(mq.toDBObject());
		if(doc==null) {
			return null;
		}
		return doc;
	}


	/* (non-Javadoc)
	 * @see com.findwise.hydra.DocumentReader#getDocuments(com.findwise.hydra.DatabaseQuery, int)
	 */
	@Override
	public List<DatabaseDocument<MongoType>> getDocuments(DatabaseQuery<MongoType> dbq, int limit) {
		DBCursor cursor = documents.find(((MongoQuery)dbq).toDBObject());

		List<DatabaseDocument<MongoType>> list = new ArrayList<DatabaseDocument<MongoType>>();
		while(list.size()<limit && cursor.hasNext()) {
			cursor.next();
			
			list.add((MongoDocument)cursor.curr());
		}
		
		return list;
	}

	public DBCollection getDocumentCollection() {
		return documents;
	}

	public void setDocumentCollection(DBCollection documents) {
		this.documents = documents;
	}

	public GridFS getDocumentFS() {
		return documentfs;
	}

	public void setDocumentFS(GridFS documentfs) {
		this.documentfs = documentfs;
	}
	

	
	/* (non-Javadoc)
	 * @see com.findwise.hydra.DocumentWriter#delete(com.findwise.hydra.DatabaseDocument)
	 */
	@Override
	public void delete(DatabaseDocument<MongoType> d) {
		BasicDBObject dbo = new BasicDBObject(MongoDocument.MONGO_ID_KEY, d.getID());
		documents.remove(dbo, concern);
	}
	
	/* (non-Javadoc)
	 * @see com.findwise.hydra.DocumentWriter#deleteAll()
	 */
	@Override
	public void deleteAll() {
		documents.remove(new BasicDBObject());
	}
	
	/* (non-Javadoc)
	 * @see com.findwise.hydra.DocumentWriter#insert(com.findwise.hydra.DatabaseDocument)
	 */
	@Override
	public boolean insert(DatabaseDocument<MongoType> d) {
		if(d.getID()==null) {
			try {
				documents.insert((MongoDocument) d, concern);
				return true;
			}
			catch (MongoException e) {
				logger.error("INSERT FAILED FOR id:"+d.getID(), e);
			}
		}
		return false;
	}
	
	private DBObject getUpdateObject(DBObject update) {
		DBObject dbo = new BasicDBObject();
		dbo.put("$set", update);
		return dbo;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.findwise.hydra.DatabaseConnector#getAndTag(com.findwise.hydra.DatabaseQuery, java.lang.String)
	 * 
	 * Modifies the mongo DatabaseQuery, call it with 
	 * getAndTag(new MongoQuery(), "tag")) unless you want to modify your query
	 * 
	 */
	@Override
	public MongoDocument getAndTag(DatabaseQuery<MongoType> query, String tag) {
		ensureIndex(tag);
		MongoQuery mq = (MongoQuery)query;
		mq.requireMetadataFieldNotExists(Document.PENDING_METADATA_FLAG);
		mq.requireMetadataFieldNotExists(FETCHED_TAG+"."+tag);
		DBObject update = new BasicDBObject(MongoDocument.METADATA_KEY+"."+FETCHED_TAG+"."+tag, new Date());
		DBObject dbo = getUpdateObject(update);

		return findAndModify(mq.toDBObject(), dbo);
	}
	
	private void ensureIndex(String tag) {
		if(!seenTags.contains(tag)) {
			long start = System.currentTimeMillis();
			documents.ensureIndex(MongoDocument.METADATA_KEY+"."+FETCHED_TAG+"."+tag);
			logger.info("Ensured index for stage "+tag+" in "+(System.currentTimeMillis()-start)+" ms");
			seenTags.add(tag);
		}
	}

	
	@Override
	public void write(DocumentFile df) throws IOException {
		QueryBuilder qb = QueryBuilder.start().put(DOCUMENT_KEY).is(df.getDocumentId());
		documentfs.remove(qb.get());
		
		GridFSInputFile input = documentfs.createFile(df.getStream(), df.getFileName());
		input.put(DOCUMENT_KEY, df.getDocumentId());
		input.save();
	}


	@Override
	public boolean update(DatabaseDocument<MongoType> d) {
		MongoDocument md = (MongoDocument) d;
		MongoQuery mdq = new MongoQuery();
		mdq.requireID(md.getID());
		
		BasicDBObjectBuilder bob = new BasicDBObjectBuilder();
		
		if(md.isActionTouched()) {
			bob.add(MongoDocument.ACTION_KEY, md.getAction().toString());
		}
		
		for(String s : md.getTouchedContent()) {
			bob.add(MongoDocument.CONTENTS_KEY+"."+s, md.getContentField(s));
		}
		for(String s : md.getTouchedMetadata()){
			bob.add(MongoDocument.METADATA_KEY+"."+s, md.getMetadataField(s));
		}
		try {
			WriteResult wr = documents.update(mdq.toDBObject(), getUpdateObject(bob.get()), false, false, concern);
			return wr.getN()==1;
		}
		catch (MongoException e) {
			logger.error("UPDATE FAILED FOR id:"+d.getID(), e);
			return false;
		}
	}


	@Override
	public boolean markTouched(Object id, String tag) {
		MongoQuery mq = new MongoQuery();
		mq.requireID(id);
		DBObject update = new BasicDBObject(MongoDocument.METADATA_KEY+"."+TOUCHED_TAG+"."+tag, new Date());
		DBObject dbo = getUpdateObject(update);
		
		if(documents.findAndModify(mq.toDBObject(), dbo)==null) {
			return false;
		}
		
		return true;
	}
	
	private DBObject getStampObject(String stage) {
		BasicDBObject object = new BasicDBObject();
		object.put(MongoDocument.DATE_METADATA_SUBKEY, new Date());
		object.put(MongoDocument.STAGE_METADATA_SUBKEY, stage);
		return object;
	}
	
	private void stampMetadataField(DBObject doc, String flag, String stage) {
		if(!doc.containsField(MongoDocument.METADATA_KEY)) {
			doc.put(MongoDocument.METADATA_KEY, new BasicDBObject());
		}
		DBObject metadata = (DBObject)doc.get(MongoDocument.METADATA_KEY);
		
		metadata.put(flag, getStampObject(stage));
		
		doc.put(MongoDocument.METADATA_KEY, metadata);
	}
	
	@Override
	public boolean markProcessed(DatabaseDocument<MongoType> d, String stage) {
		MongoQuery mq = new MongoQuery();
		mq.requireID(d.getID());
		DBObject doc = documents.findAndRemove(mq.toDBObject());
		
		if(doc==null) {
			return false;
		}
		
		stampMetadataField(doc, MongoDocument.PROCESSED_METADATA_FLAG, stage);
		
		oldDocuments.insert(doc);
		
		return true;
	}
	
	@Override
	public boolean markDiscarded(DatabaseDocument<MongoType> d, String stage) {
		MongoQuery mq = new MongoQuery();
		mq.requireID(d.getID());
		DBObject doc = documents.findAndRemove(mq.toDBObject());
		
		if(doc==null) {
			return false;
		}
		
		stampMetadataField(doc, MongoDocument.DISCARDED_METADATA_FLAG, stage);

		oldDocuments.insert(doc);
		
		return true;
	}
	
	@Override
	public boolean markPending(DatabaseDocument<MongoType> d, String stage) {
		MongoQuery mq = new MongoQuery();
		mq.requireID(d.getID());
		DBObject update = new BasicDBObject();
		update.put(MongoDocument.METADATA_KEY+"."+MongoDocument.PENDING_METADATA_FLAG+"."+MongoDocument.DATE_METADATA_SUBKEY, new Date());
		update.put(MongoDocument.METADATA_KEY+"."+MongoDocument.PENDING_METADATA_FLAG+"."+MongoDocument.STAGE_METADATA_SUBKEY, stage);
		DBObject dbo = getUpdateObject(update);
		
		if(documents.findAndModify(mq.toDBObject(), dbo)==null) {
			return false;
		}
		
		return true;
	}


	@Override
	public DatabaseDocument<MongoType> getAndTagRecurring(DatabaseQuery<MongoType> query, String tag) {
		return getAndTagRecurring(query, tag, DEFAULT_RECURRING_INTERVAL);
	}
	
	@Override
	public MongoDocument getAndTagRecurring(DatabaseQuery<MongoType> query, String tag, int intervalMillis) {
		MongoDocument md = getAndTag(query, tag);
		if(md!=null) {
			return md;
		}
		
		MongoQuery mq = (MongoQuery)query;
		BasicDBObjectBuilder dbob = BasicDBObjectBuilder.start(mq.toDBObject().toMap());
		dbob.add(MongoDocument.METADATA_KEY+"."+Document.PENDING_METADATA_FLAG, new BasicDBObject("$exists", false));
		
		Date earlierThan = new Date(new Date().getTime()-intervalMillis);
		dbob.add(MongoDocument.METADATA_KEY+"."+FETCHED_TAG+"."+tag, new BasicDBObject("$lt", earlierThan));
		
		DBObject update = new BasicDBObject(MongoDocument.METADATA_KEY+"."+FETCHED_TAG+"."+tag, new Date());
		
		return findAndModify(dbob.get(), getUpdateObject(update));
	}

	
	private MongoDocument findAndModify(DBObject query, DBObject modification) {
		DBObject c = (DBObject)documents.findAndModify(query, modification);
		
		if(c==null) {
			return null;
		}
		
		MongoDocument md = new MongoDocument();
		md.putAll(c);
		
		return md;
	}

	@Override
	public long getActiveDatabaseSize() {
		return documents.count();
	}
	
	@Override
	public long getInactiveDatabaseSize() {
		return oldDocuments.count();
	}
}
