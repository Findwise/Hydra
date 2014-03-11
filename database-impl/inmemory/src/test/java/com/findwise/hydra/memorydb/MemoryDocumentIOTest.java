package com.findwise.hydra.memorydb;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.DatabaseQuery;
import com.findwise.hydra.Document;
import com.findwise.hydra.Document.Status;
import com.findwise.hydra.DocumentFile;
import com.findwise.hydra.SerializationUtils;
import org.mockito.Matchers;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

@RunWith(RepeatRunner.class)
public class MemoryDocumentIOTest {
	private MemoryDocumentIO io;
	
	private MemoryDocument test;
	private MemoryDocument test2;
	
	@Before
	public void setUp() {
		io = new MemoryDocumentIO();
		
		test = TestTools.getRandomDocument();
		test.putContentField("name", "test");
		test.putContentField("number", 1);
		test2 = TestTools.getRandomDocument();
		test2.putContentField("name", "test");
		test2.putContentField("number", 2);
		io.insert(test);
		io.insert(test2);
	}
	
	@Test
	@Repeat(50)
	public void testInsertDocument() {
		long size = io.getActiveDatabaseSize();
		
		int count = new Random().nextInt(200);
		for(int i = 0; i<count; i++) {
			MemoryDocument md = TestTools.getRandomDocument();
			
			io.insert(md);
		}
		
		
		if(io.getActiveDatabaseSize()!=size+count) {
			fail("Incorrect database size after inserts. Expected "+(size+count)+" but found "+io.getActiveDatabaseSize());
		}
	}

	@Test
	public void testInsertWithAttachments() throws Exception {
		MemoryDocument md = TestTools.getRandomDocument();

		byte[] inputData = new byte[]{1,2,3};
		DocumentFile<MemoryType> documentFile = buildSimpleDocumentFile(inputData);
		List<DocumentFile<MemoryType>> l = new ArrayList<DocumentFile<MemoryType>>();
		l.add(documentFile);
		io.insert(md, l);

		/* First of all, we should be able to fetch the attachment */
		DocumentFile outputDocFile = io.getDocumentFile(md, documentFile.getFileName());
		assertNotNull(outputDocFile);

		/* And it should have the correct data */
		byte[] outputData = IOUtils.toByteArray(outputDocFile.getStream());
		assertArrayEquals(inputData, outputData);

		/* We have changed the state of the documentFile we sent in (for better or worse) */
		assertEquals(md.getID(), documentFile.getDocumentId());

		/* The document in mongo has a metadata field reflecting the fact that it is fully committed */
		MemoryDocument outputDocument = io.getDocumentById(md.getID());
		assertFalse((Boolean) outputDocument.getMetadataField(Document.COMMITTING_METADATA_FLAG));
	}

	@Test
	public void testInsertWithAttachmentsTwoStageCommit() throws Exception {
		io = spy(io);
		MemoryDocument md = TestTools.getRandomDocument();

		// Assume that writing a DocumentFile raises an exception
		doThrow(new RuntimeException()).when(io).write(Matchers.<DocumentFile<MemoryType>>any());
		List<DocumentFile<MemoryType>> l = new ArrayList<DocumentFile<MemoryType>>();
		l.add(buildSimpleDocumentFile(new byte[]{}));
		try {
			io.insert(md, l);
		} catch (RuntimeException e) {
		}

		/* Then the database document has metadata reflecting that it failed */
		MemoryDocument outputDocument = io.getDocumentById(md.getID());
		assertTrue((Boolean) outputDocument.getMetadataField(Document.COMMITTING_METADATA_FLAG));
	}

	private DocumentFile<MemoryType> buildSimpleDocumentFile(byte[] bytes) throws UnsupportedEncodingException {
		ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
		return new DocumentFile<MemoryType>(null, "filename", inputStream);
	}

	@Test
	public void testUpdateDocument() {
		String field = "xyz";
		String content = "zyx";
		MemoryQuery mdq = new MemoryQuery();
		mdq.requireContentFieldEquals("name", "test");
		DatabaseDocument<MemoryType> d = io.getDocument(mdq);
		d.putContentField(field, content);
		
		io.update(d);
		mdq = new MemoryQuery();
		mdq.requireContentFieldEquals(field, content);
		d = io.getDocument(mdq);
		
		if(!d.getContentField("xyz").equals("zyx")) {
			fail("Wrong data in updated field");
		}
	}

	@Test
	public void getDocuments() {
		MemoryQuery mdq = new MemoryQuery();
		List<DatabaseDocument<MemoryType>> list = io.getDocuments(mdq, 2);
		if(list.size()!=2) {
			fail("Did not return all documents. Expected 2, found "+list.size());
		}
		boolean test1 = false, test2=false;
		for(Document<MemoryType> d : list) {
			if("test".equals(d.getContentField("name"))) {
				if(((Integer)1).equals(d.getContentField("number")))
				{
					test1 = true;
				}
				else if(((Integer)2).equals(d.getContentField("number"))) {
					test2 = true;
				}
			}
		}
		if (!test1 || !test2 ) {
			fail("Not all three documents were found");
		}
		
		mdq.requireContentFieldEquals("name", "test");
		list = io.getDocuments(mdq, 3);
		if(list.size()!=2) {
			fail("Wrong number of documents returned. Expected 2, found "+list.size());
		}
		
		mdq.requireContentFieldEquals("number", 2);
		list = io.getDocuments(mdq, 1);
		if(list.size()!=1) {
			fail("Wrong number of documents returned. Expected 1, found "+list.size());
		}
	}

	@Test
	public void testIdSerialization() throws Exception {
		MemoryDocument md = new MemoryDocument();
		io.insert(md);
		String serialized = SerializationUtils.toJson(md.getID().getID());
		Object deserialized = io.toDocumentIdFromJson(serialized).getID();
		if(!md.getID().getID().equals(deserialized)) {
			fail("Serialization failed from json for "+md.getID().getID().toString() );
		}
		
		deserialized = io.toDocumentId(SerializationUtils.toObject(serialized)).getID();
		if(!md.getID().getID().equals(deserialized)) {
			fail("Serialization failed from primitive");
		}
	}

	@Test
	public void testWriteDocumentFile() throws IOException{
		if(io.getDocumentFileNames(test2).size()!=0) {
			fail("found file already attached to the document");
		}
		
		String content = TestTools.getRandomString(100);
		String name = TestTools.getRandomString(10);
		DocumentFile<MemoryType> df = new DocumentFile<MemoryType>(test2.getID(), name, IOUtils.toInputStream(content), "stage");

		io.write(df);
		DocumentFile<MemoryType> df2 = io.getDocumentFile(test2, name);

		if(!IOUtils.toString(df2.getStream()).equals(content)) {
			fail("Content mismatch between saved and fetched file");
		}
		
		df.getStream().close();
		df2.getStream().close();
	}
	
	@Test
	public void testGetDocumentDatabaseQuery() {
		DatabaseQuery<MemoryType> dbq = new MemoryQuery();
		dbq.requireContentFieldEquals("name", "test");
		dbq.requireContentFieldEquals("number", 2);
		MemoryDocument d = io.getDocument(dbq);
		
		if (d == null) {
			fail("Did not find any documents matching query");
		}

		if(d.isEqual(test) || !d.isEqual(test2)) {
			fail("Incorrect document returned");
		}
		
	}

	@Test
	public void testGetAndTagDocumentDatabaseQueryString() {
		MemoryQuery mdq = new MemoryQuery();
		mdq.requireContentFieldExists("name");
		Document<MemoryType> d = io.getAndTag(mdq, "tag");
		if(d==null) {
			fail("Get and tag could not find any document");
		}
		if(d.getID()==null) {
			fail("Get and tag didn't get a document with a set ID");
		}
		if(d.getContentFields().size()==0) {
			fail("Get and tag didn't get any contents in the document");
		}
		if(d.getMetadataMap().size()==0) {
			fail("Get and tag didn't get any metadata in the document");
		}
		io.getAndTag(new MemoryQuery(), "tag");
		io.getAndTag(new MemoryQuery(), "tag");
		
		d = io.getAndTag(new MemoryQuery(), "tag");
		if(d!=null) {
			fail("Expected all documents to be tagged, but found "+d);
		}
	}


	@SuppressWarnings("unchecked")
	@Test
	public void testFetchRemoval() throws Exception {
		MemoryDocument md = new MemoryDocument();
		md.putContentField("field", "value");
		MemoryDocumentIO io = new MemoryDocumentIO();
		io.insert(md);

		io.getAndTag(new MemoryQuery(), "tag");

		MemoryDocument d2 = io.getDocumentById(md.getID());
		
		assertTrue(d2.getMetadataMap().containsKey(MemoryDocument.FETCHED_METADATA_TAG));
		Map<String, Object> fetched = (Map<String, Object>)d2.getMetadataMap().get(MemoryDocument.FETCHED_METADATA_TAG);
		
		assertTrue(fetched.containsKey("tag"));
		fetched.remove("tag");
		
		MemoryDocument d3 = new MemoryDocument();
		d3.fromJson(d2.toJson());
		io.update(d3);

		assertFalse((
				(Map<String, Object>) io.getDocumentById(d2.getID()).getMetadataMap().get(MemoryDocument.FETCHED_METADATA_TAG))
				.containsKey("tag"));
	}

	@Test
	public void testDelete() {
		io.delete(test);
		if(io.getDocumentById(test.getID())!=null) {
			fail("Document failed to be deleted.");
		}
	}
	
	@Test
	public void testDiscardDocument() {		
		DatabaseDocument<MemoryType> discarded_d = io.getAndTag(new MemoryQuery(), "DiscardedTag");
		
		DatabaseDocument<MemoryType> d;
		List<Object> allDocs = new ArrayList<Object>();
		while ((d = io.getAndTag(new MemoryQuery(), "testRet")) != null) {
			allDocs.add(d.getID());
		}
		if (!allDocs.contains(discarded_d.getID())) {
			fail("Discarded document should still be there since it's not yet discarded");
		}
		
		io.markDiscarded(discarded_d, "test_stage");
		
		while ((d = io.getAndTag(new MemoryQuery(), "testGetNonDiscarded")) != null) {
			if(discarded_d.getID() == d.getID()) {
				fail("Discarded document was retrieved after discard");
			}
		}
		
		DatabaseDocument<MemoryType> old = io.getDocumentById(discarded_d.getID(), true);
		if(old==null) {
			fail("Failed to find the document in the 'old' database");
		}
		
		if(old.getStatus()!=Status.DISCARDED) {
			fail("No discarded flag on the document");
		}
	}
	
	@Test
	public void testNullFields() {
		io.deleteAll();
		MemoryDocument md = new MemoryDocument();
		md.putContentField("field", "value");
		md.putContentField("nullfield", null);
		io.insert(md);
		MemoryDocument indb = io.getAndTag(new MemoryQuery(), "tag");

		if(indb.hasContentField("nullfield")) {
			fail("Null field was persisted in database on insert");
		}
		assertEquals("value", indb.getContentField("field"));
		
		md.putContentField("field", null);
		
		io.update(md);

		indb = io.getAndTag(new MemoryQuery(), "tag2");

		if(indb.hasContentField("field")) {
			fail("Null field was persisted in database on update");
		}

	}
	
	@Test
	public void testFailedDocument() {		
		DatabaseDocument<MemoryType> failed = io.getAndTag(new MemoryQuery(), "failedTag");
		
		io.markFailed(failed, "test_stage");
		
		if(io.getDocumentById(failed.getID())!=null) {
			fail("Failed document was retrieved after discard");
		}
		
		DatabaseDocument<MemoryType> old = io.getDocumentById(failed.getID(), true);
		if(old==null) {
			fail("Failed to find the document in the 'old' database");
		}
		
		if(old.getStatus()!=Status.FAILED) {
			fail("No failed flag on the document");
		}
	}
	
	@Test
	public void testPendingDocument() {		
		DatabaseDocument<MemoryType> pending = io.getAndTag(new MemoryQuery(), "pending");
		
		io.markPending(pending, "test_stage");
		
		pending = io.getDocumentById(pending.getID());
		
		if(pending.getStatus()!=Status.PENDING) {
			fail("No PENDING flag on the document");
		}
	}
	
	@Test
	public void testProcessedDocument() {		
		DatabaseDocument<MemoryType> processed = io.getAndTag(new MemoryQuery(), "processed");
		
		io.markProcessed(processed, "test_stage");
		
		processed = io.getDocumentById(processed.getID(), true);
		
		if(processed.getStatus()!=Status.PROCESSED) {
			fail("No PROCESSED flag on the document");
		}
	}
	
	@Test
	public void testActiveDatabaseSize() {
		if(io.getActiveDatabaseSize() != 2) {
			fail("Not the correct active database size. Expected 2 got: "+io.getActiveDatabaseSize());
		}
		List<DatabaseDocument<MemoryType>> list = io.getDocuments(new MemoryQuery(), 2);
		io.markProcessed(list.get(0), "x");
		io.markDiscarded(list.get(1), "x");
		if(io.getActiveDatabaseSize() != 0) {
			fail("Not the correct active database size after processed and discard, expected 0 found " + io.getActiveDatabaseSize());
		}
	}
	
}
