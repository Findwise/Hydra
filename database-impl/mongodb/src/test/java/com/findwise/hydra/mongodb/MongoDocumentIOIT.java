package com.findwise.hydra.mongodb;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.bson.types.ObjectId;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.Document;
import com.findwise.hydra.Document.Status;
import com.findwise.hydra.DocumentFile;
import com.findwise.hydra.DocumentID;
import com.findwise.hydra.DocumentWriter;
import com.findwise.hydra.SerializationUtils;
import com.findwise.hydra.TailableIterator;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.WriteConcern;

public class MongoDocumentIOIT {
	
	@Rule
	public MongoConnectorResource mongoConnectorResource = new MongoConnectorResource(getClass());
	
	private Random r = new Random(System.currentTimeMillis());

	@Test
	public void testPrepare() {
		MongoConnector mdc = mongoConnectorResource.getConnector();
		DB db = mdc.getDB();
		db.getCollection(MongoDocumentIO.OLD_DOCUMENT_COLLECTION).drop();
		
		if(db.getCollectionNames().contains(MongoDocumentIO.OLD_DOCUMENT_COLLECTION)) {
			fail("Collection already exists");
		}
		mdc.getDocumentWriter().prepare();
		
		if(!db.getCollectionNames().contains(MongoDocumentIO.OLD_DOCUMENT_COLLECTION)) {
			fail("Collection was not created");
		}
		
		if(!isCapped()) {
			fail("Collection not capped");
		}
	}
	
	private boolean isCapped() {
		MongoConnector mdc = mongoConnectorResource.getConnector();
		return mdc.getDB().getCollection(MongoDocumentIO.OLD_DOCUMENT_COLLECTION).isCapped();
	}
	
	@Test
	public void testConnectPrepare() throws Exception {
		MongoConnector mdc = mongoConnectorResource.getConnector();
		mdc.getDB().dropDatabase();
		while(mdc.getDB().getCollection(MongoStatusIO.HYDRA_COLLECTION_NAME).count()!=0) {
			mdc.getDB().getCollection(MongoStatusIO.HYDRA_COLLECTION_NAME).remove(new BasicDBObject(), WriteConcern.SAFE);
			Thread.sleep(50);
		}
		
		if(mdc.getStatusReader().hasStatus()) {
			fail("Test error");
		}
		
		assertFalse(isCapped());
		
		mdc.connect();

		if(!isCapped()) {
			fail("Collection was not capped on connect");
		}
	}
	
	@Test
	public void testRollover() throws Exception {
		MongoConnector mdc = mongoConnectorResource.getConnector();
		DocumentWriter<MongoType> dw = mdc.getDocumentWriter();
		dw.prepare();

		for(int i=0; i<mdc.getStatusReader().getStatus().getNumberToKeep(); i++) {
			dw.insert(new MongoDocument());
			DatabaseDocument<MongoType> dd = dw.getAndTag(new MongoQuery(), "tag");
			dw.markProcessed(dd, "tag");
		}
		
		if(mdc.getDocumentReader().getActiveDatabaseSize()!=0) {
			fail("Still some active docs..");
		}
		
		if(mdc.getDocumentReader().getInactiveDatabaseSize()!=mdc.getStatusReader().getStatus().getNumberToKeep()) {
			fail("Incorrect number of old documents kept");
		}
		
		dw.insert(new MongoDocument());
		DatabaseDocument<MongoType> dd = dw.getAndTag(new MongoQuery(), "tag");
		dw.markProcessed(dd, "tag");
		
		if(mdc.getDocumentReader().getActiveDatabaseSize()!=0) {
			fail("Still some active docs..");
		}
		if(mdc.getDocumentReader().getInactiveDatabaseSize()!=mdc.getStatusReader().getStatus().getNumberToKeep()) {
			fail("Incorrect number of old documents kept: "+ mdc.getDocumentReader().getInactiveDatabaseSize());
		}
	}
	
	@Test
	public void testNullFields() throws Exception {
		MongoConnector mdc = mongoConnectorResource.getConnector();
		MongoDocumentIO dw = mdc.getDocumentWriter();
		MongoDocument md = new MongoDocument();
		md.putContentField("field", "value");
		md.putContentField("nullfield", null);
		dw.insert(md);
		
		MongoDocument indb = dw.getAndTag(new MongoQuery(), "tag");
		
		if(indb.hasContentField("nullfield")) {
			fail("Null field was persisted in database on insert");
		}
		assertEquals("value", indb.getContentField("field"));
		
		md.putContentField("field", null);
		
		dw.update(md);

		indb = dw.getAndTag(new MongoQuery(), "tag2");

		if(indb.hasContentField("field")) {
			fail("Null field was persisted in database on update");
		}
		
	}
	
	@Test
	public void testIdSerialization() throws Exception {
		MongoConnector mdc = mongoConnectorResource.getConnector();
		ObjectId id = new ObjectId();
		
		String serialized = SerializationUtils.toJson(id);
		DocumentID<MongoType> deserialized = mdc.getDocumentReader().toDocumentIdFromJson(serialized);
		if(!id.equals(deserialized.getID())) {
			fail("Serialization failed from json string");
		}
		deserialized = mdc.getDocumentReader().toDocumentId(SerializationUtils.toObject(serialized));
		if(!id.equals(deserialized.getID())) {
			fail("Serialization failed from primitive");
		}
	}
	
	@Test
	public void testInactiveIterator() throws Exception {
		MongoConnector mdc = mongoConnectorResource.getConnector();
		DocumentWriter<MongoType> dw = mdc.getDocumentWriter();
		dw.prepare();
		
		TailableIterator<MongoType> it = mdc.getDocumentReader().getInactiveIterator();
		
		TailReader tr = new TailReader(it);
		tr.start();
		
		MongoDocument first = new MongoDocument();
		first.putContentField("num", 1);
		dw.insert(first);
		DatabaseDocument<MongoType> dd = dw.getAndTag(new MongoQuery(), "tag");
		dw.markProcessed(dd, "tag");
		
		while(tr.lastRead>System.currentTimeMillis() && tr.isAlive()) {
			Thread.sleep(50);
		}
		
		if(!tr.isAlive()) {
			fail("TailableReader died");
		}
		
		long lastRead = tr.lastRead;
		
		if(!tr.lastReadDoc.getContentField("num").equals(1)) {
			fail("Last doc read was not the correct document!");
		}
		
		MongoDocument second = new MongoDocument();
		second.putContentField("num", 2);
		dw.insert(second);
		dd = dw.getAndTag(new MongoQuery(), "tag");
		dw.markProcessed(dd, "tag");
		
		while(tr.lastRead==lastRead) {
			Thread.sleep(50);
		}

		if (!tr.lastReadDoc.getContentField("num").equals(2)) {
			fail("Last doc read was not the correct document!");
		}

		
		if(tr.hasError) {
			fail("An exception was thrown by the TailableIterator prior to interrupt");
		}
		
		tr.interrupt();

		long interrupt = System.currentTimeMillis();
		
		while (tr.isAlive() && (System.currentTimeMillis()-interrupt)<10000) {
			Thread.sleep(50);
		}
		
		if(tr.isAlive()) {
			fail("Unable to interrupt the tailableiterator");
		}
		
		if(tr.hasError) {
			fail("An exception was thrown by the TailableIterator after interrupt");
		}
	}
	
	@Test
	public void testDoneContentTransfer() throws Exception {
		MongoConnector mdc = mongoConnectorResource.getConnector();
		mdc.getDocumentWriter().prepare();
		
		MongoDocument d = new MongoDocument();
		d.putContentField(getRandomString(5), getRandomString(20));
		d.putContentField(getRandomString(5), getRandomString(20));
		d.putContentField(getRandomString(5), getRandomString(20));
		d.putContentField(getRandomString(5), getRandomString(20));
		d.putContentField(getRandomString(5), getRandomString(20));
		
		mdc.getDocumentWriter().insert(d);

		d = mdc.getDocumentReader().getDocumentById(d.getID());
		
		d.putContentField(getRandomString(5), getRandomString(20));
		
		mdc.getDocumentWriter().update(d);
		
		mdc.getDocumentWriter().markProcessed(d, "x");
		
		MongoDocument d2 = mdc.getDocumentReader().getDocumentById(d.getID(), true);
		
		if(d.getContentFields().size()!=d2.getContentFields().size()) {
			fail("Processed document did not have the correct number of content fields");
		}
		
		for(String field : d.getContentFields()) {
			if(!d2.hasContentField(field)) {
				fail("Processed document did not have the correct content fields");
			}
			
			if(!d2.getContentField(field).equals(d.getContentField(field))) {
				fail("Processed document did not have the correct data in the content fields");
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testInsertWithAttachments() throws Exception {
		MongoConnector mdc = mongoConnectorResource.getConnector();
		MongoDocumentIO dw = mdc.getDocumentWriter();
		MongoDocument md = new MongoDocument();

		byte[] inputData = new byte[]{1,2,3};
		DocumentFile<MongoType> documentFile = buildSimpleDocumentFile(inputData);
		dw.insert(md, Arrays.asList(documentFile));

		/* First of all, we should be able to fetch the document file */
		DocumentFile<MongoType> outputDocFile = dw.getDocumentFile(md, documentFile.getFileName());
		assertNotNull(outputDocFile);

		/* And it should have the correct data */
		byte[] outputData = IOUtils.toByteArray(outputDocFile.getStream());
		assertArrayEquals(inputData, outputData);

		/* We have changed the documentId of the documentFile we sent in (for better or worse) */
		assertEquals(md.getID(), documentFile.getDocumentId());

		/* The document in mongo has "committing" metadata set to false (is fully committed) */
		MongoDocument outputDocument = mdc.getDocumentReader().getDocumentById(md.getID());
		assertFalse((Boolean) outputDocument.getMetadataField(Document.COMMITTING_METADATA_FLAG));
	}

	@SuppressWarnings("unchecked")
	@Test
	/* Test that we *can* get documents that are fully committed */
	public void testFullyCommittedDocumentsCanBeTagged() throws Exception {
		MongoConnector mdc = mongoConnectorResource.getConnector();
		MongoDocumentIO dw = mdc.getDocumentWriter();
		MongoDocument md = new MongoDocument();
		dw.insert(md, Arrays.asList(buildSimpleDocumentFile(new byte[]{1, 2, 3})));
		MongoQuery mongoQuery = new MongoQuery();
		mongoQuery.requireID(md.getID());
		assertNotNull(dw.getAndTag(mongoQuery));
	}

	@SuppressWarnings("unchecked")
	@Test
	/* Test that we *can't* get documents that are *not* fully committed */
	public void testCommittingDocumentsCannotBeTagged() throws Exception {
		MongoConnector mdc = mongoConnectorResource.getConnector();
		MongoDocumentIO dw = spy(mdc.getDocumentWriter());
		MongoDocument md = new MongoDocument();
		doThrow(new RuntimeException()).when(dw).write(any(DocumentFile.class)); // Unchecked due to generics
		try {
			List<DocumentFile<MongoType>> docFiles = new ArrayList<DocumentFile<MongoType>>();
			docFiles.add(buildSimpleDocumentFile(new byte[]{1, 2, 3}));
			dw.insert(md, docFiles);
		} catch( RuntimeException e) {}
		MongoQuery mongoQuery = new MongoQuery();
		mongoQuery.requireID(md.getID());
		assertNull(dw.getAndTag(mongoQuery));
	}

	private DocumentFile<MongoType> buildSimpleDocumentFile(byte[] bytes) throws UnsupportedEncodingException {
		ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
		return new DocumentFile<MongoType>(null, "filename", inputStream);
	}

	@Test
	public void testFetchRemoval() throws Exception {
		MongoConnector mdc = mongoConnectorResource.getConnector();
		MongoDocumentIO dw = mdc.getDocumentWriter();
		MongoDocument md = new MongoDocument();
		md.putContentField("field", "value");
		dw.insert(md);

		MongoQuery mongoQuery = new MongoQuery();
		mongoQuery.requireID(md.getID());
		dw.getAndTag(mongoQuery, "tag");

		MongoDocument d2 = dw.getDocumentById(md.getID());

		assertNotNull(d2.getFetchedBy());
		
		
		assertTrue(d2.fetchedBy("tag"));
		d2.removeFetchedBy("tag");
		dw.update(new MongoDocument(d2.toJson()));
		
		assertFalse(dw.getDocumentById(d2.getID()).fetchedBy("tag"));
	}
	
	@Ignore
	@Test
	public void testInsertLargeDocument() throws Exception {
		MongoConnector mdc = mongoConnectorResource.getConnector();
		DocumentWriter<MongoType> dw = mdc.getDocumentWriter();
		dw.prepare();
		
		MongoDocument d = new MongoDocument();
		makeDocumentTooLarge(d);
		
		if(dw.insert(d)) {
			fail("No error inserting big document");
		}
	}
	
	@Test
	@Ignore
	public void testUpdateLargeDocument() throws Exception {
		MongoConnector mdc = mongoConnectorResource.getConnector();
		DocumentWriter<MongoType> dw = mdc.getDocumentWriter();
		dw.prepare();
		
		
		
		MongoDocument d = new MongoDocument();
		d.putContentField("some_field", "some data");
		
		dw.insert(d);
		
		makeDocumentTooLarge(d);
		
		if(dw.update(d)) {
			fail("No error updating big document");
		}
	}
	
	private void makeDocumentTooLarge(MongoDocument d) {
		MongoConnector mdc = mongoConnectorResource.getConnector();
		int maxMongoDBObjectSize = mdc.getDB().getMongo().getConnector().getMaxBsonObjectSize();
		while(d.toJson().getBytes().length <= maxMongoDBObjectSize) {
			d.putContentField(getRandomString(5), getRandomString(1000000));
		}
	}
	
	int testReadCount = 1;
	@Test
	public void testReadStatus() throws Exception {
		MongoConnector mdc = mongoConnectorResource.getConnector();
		mdc.getDocumentWriter().prepare();
		
		testReadCount = (int)mdc.getStatusReader().getStatus().getNumberToKeep(); 
		
		TailReader tr = new TailReader(mdc.getDocumentReader().getInactiveIterator());
		tr.start();
		
		Thread t = new Thread() {
			public void run() {
				try {
					insertDocuments(testReadCount);
					processDocuments(testReadCount/3);
					failDocuments(testReadCount/3);
					discardDocuments(testReadCount - (testReadCount/3)*2);
				} catch (Exception e) {
					System.out.println(e.getMessage());
				}
			}
		};
		t.start();
		
		long timer = System.currentTimeMillis();
		
		while (tr.count < testReadCount && (System.currentTimeMillis()-timer)<10000) {
			Thread.sleep(50);
		}
		
		if(tr.count < testReadCount) {
			fail("Did not see all documents");
		}
		
		if(tr.count > testReadCount) {
			fail("Saw too many documents");
		}
		
		if(tr.countProcessed != testReadCount/3) {
			fail("Incorrect number of processed documents. Expected "+testReadCount/3+" but saw "+tr.countProcessed);
		}
		
		if(tr.countFailed != testReadCount/3) {
			fail("Incorrect number of failed documents. Expected "+testReadCount/3+" but saw "+tr.countFailed);
		}
		
		if(tr.countDiscarded != testReadCount - (testReadCount/3)*2) {
			fail("Incorrect number of discarded documents. Expected "+(testReadCount - (testReadCount/3)*2)+" but saw "+tr.countDiscarded);
		}
		
		tr.interrupt();
	}
	
	public long processDocuments(int count) throws Exception {
		MongoConnector mdc = mongoConnectorResource.getConnector();
		long start = System.currentTimeMillis();
		DatabaseDocument<MongoType> dd;
		for(int i=0; i<count; i++) {
			dd = mdc.getDocumentReader().getDocument(new MongoQuery());
			mdc.getDocumentWriter().markProcessed(dd, "x");
		}
		return System.currentTimeMillis()-start;
	}
	
	public long failDocuments(int count) throws Exception {
		MongoConnector mdc = mongoConnectorResource.getConnector();
		long start = System.currentTimeMillis();
		DatabaseDocument<MongoType> dd;
		for(int i=0; i<count; i++) {
			dd = mdc.getDocumentReader().getDocument(new MongoQuery());
			mdc.getDocumentWriter().markFailed(dd, "x");
		}
		return System.currentTimeMillis()-start;
	}
	
	public long discardDocuments(int count) throws Exception {
		MongoConnector mdc = mongoConnectorResource.getConnector();
		long start = System.currentTimeMillis();
		DatabaseDocument<MongoType> dd;
		for(int i=0; i<count; i++) {
			dd = mdc.getDocumentReader().getDocument(new MongoQuery());
			mdc.getDocumentWriter().markDiscarded(dd, "x");
		}
		return System.currentTimeMillis()-start;
	}
	
	public long insertDocuments(int count) throws Exception {
		MongoConnector mdc = mongoConnectorResource.getConnector();
		long start = System.currentTimeMillis();
		for(int i=0; i<count; i++) {
			MongoDocument d = new MongoDocument();
			d.putContentField(getRandomString(5), getRandomString(20));
			mdc.getDocumentWriter().insert(d);
		}
		return System.currentTimeMillis()-start;
	}

	
	private String getRandomString(int length) {
		char[] ca = new char[length];

		for (int i = 0; i < length; i++) {
			ca[i] = (char) ('A' + r.nextInt(26));
		}

		return new String(ca);
	}
	

	public static class TailReader extends Thread {
		private TailableIterator<MongoType> it;
		public long lastRead = Long.MAX_VALUE;
		public DatabaseDocument<MongoType> lastReadDoc = null;
		boolean hasError = false;
		
		int countFailed = 0;
		int countProcessed = 0;
		int countDiscarded = 0;
		
		int count = 0;
		
		public TailReader(TailableIterator<MongoType> it) {
			this.it = it;
		}

		public void run() {
			try {
				while (it.hasNext()) {
					lastRead = System.currentTimeMillis();
					lastReadDoc = it.next();
					
					Status s = lastReadDoc.getStatus();
					
					if(s==Status.DISCARDED) {
						countDiscarded++;
					} else if (s == Status.PROCESSED) {
						countProcessed++;
					} else if (s == Status.FAILED) {
						countFailed++;
					}
					
					count++;
				}
			} catch (Exception e) {
				e.printStackTrace();
				hasError = true;
			}
		}

		public void interrupt() {
			it.interrupt();
		}
	}
}
