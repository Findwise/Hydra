package com.findwise.hydra.mongodb;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.bson.types.ObjectId;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import com.findwise.hydra.Document;
import com.findwise.hydra.DocumentFile;
import com.findwise.hydra.DocumentID;
import com.findwise.hydra.DocumentWriter;
import com.findwise.hydra.SerializationUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.WriteConcern;
import org.mockito.Matchers;

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

	@Test
	public void testInsertWithAttachments() throws Exception {
		MongoConnector mdc = mongoConnectorResource.getConnector();
		MongoDocumentIO dw = mdc.getDocumentWriter();
		MongoDocument md = new MongoDocument();

		byte[] inputData = new byte[]{1,2,3};
		DocumentFile<MongoType> documentFile = buildSimpleDocumentFile(inputData);
		List<DocumentFile<MongoType>> l = new ArrayList<DocumentFile<MongoType>>();
		l.add(documentFile);
		dw.insert(md, l);

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

	@Test
	/* Test that we *can* get documents that are fully committed */
	public void testFullyCommittedDocumentsCanBeTagged() throws Exception {
		MongoConnector mdc = mongoConnectorResource.getConnector();
		MongoDocumentIO dw = mdc.getDocumentWriter();
		MongoDocument md = new MongoDocument();
		List<DocumentFile<MongoType>> l = new ArrayList<DocumentFile<MongoType>>();
		l.add(buildSimpleDocumentFile(new byte[]{1, 2, 3}));
		dw.insert(md, l);
		MongoQuery mongoQuery = new MongoQuery();
		mongoQuery.requireID(md.getID());
		assertNotNull(dw.getAndTag(mongoQuery));
	}

	@Test
	/* Test that we *can't* get documents that are *not* fully committed */
	public void testCommittingDocumentsCannotBeTagged() throws Exception {
		MongoConnector mdc = mongoConnectorResource.getConnector();
		MongoDocumentIO dw = spy(mdc.getDocumentWriter());
		MongoDocument md = new MongoDocument();
		doThrow(new RuntimeException()).when(dw).write(Matchers.<DocumentFile<MongoType>>any());
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
		int fieldNameSize = 8;
		int fieldSize = 1024*1024;
		for (int i = 0 ; i <= maxMongoDBObjectSize / (fieldNameSize + fieldSize) ; i++) {
			d.putContentField(getRandomString(fieldNameSize), getRandomString(fieldSize));
		}
	}

	@Test
	public void testReadStatus() throws Exception {
		MongoConnector mdc = mongoConnectorResource.getConnector();
		mdc.getDocumentWriter().prepare();

		final int testReadCount = (int)mdc.getStatusReader().getStatus().getNumberToKeep();
		
		TailReaderThread tailReaderThread = new TailReaderThread(mdc.getDocumentReader().getInactiveIterator());
		tailReaderThread.start();
		
		DocumentInserterThread documentInserterThread = new DocumentInserterThread(testReadCount, mdc);
		documentInserterThread.start();
		
		long timer = System.currentTimeMillis();
		
		while (tailReaderThread.count < testReadCount && (System.currentTimeMillis()-timer)<10000) {
			Thread.sleep(50);
		}
		
		if(tailReaderThread.count < testReadCount) {
			fail("Did not see all documents");
		}
		
		if(tailReaderThread.count > testReadCount) {
			fail("Saw too many documents");
		}
		
		if(tailReaderThread.countProcessed != testReadCount/3) {
			fail("Incorrect number of processed documents. Expected "+testReadCount/3+" but saw "+tailReaderThread.countProcessed);
		}
		
		if(tailReaderThread.countFailed != testReadCount/3) {
			fail("Incorrect number of failed documents. Expected "+testReadCount/3+" but saw "+tailReaderThread.countFailed);
		}
		
		if(tailReaderThread.countDiscarded != testReadCount - (testReadCount/3)*2) {
			fail("Incorrect number of discarded documents. Expected "+(testReadCount - (testReadCount/3)*2)+" but saw "+tailReaderThread.countDiscarded);
		}
		
		tailReaderThread.interrupt();
	}

	private String getRandomString(int length) {
		char[] ca = new char[length];

		for (int i = 0; i < length; i++) {
			ca[i] = (char) ('A' + r.nextInt(26));
		}

		return new String(ca);
	}
}
