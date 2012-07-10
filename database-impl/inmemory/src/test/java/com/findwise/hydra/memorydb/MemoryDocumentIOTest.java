package com.findwise.hydra.memorydb;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.DatabaseQuery;
import com.findwise.hydra.common.Document;
import com.findwise.hydra.common.Document.Status;
import com.findwise.hydra.common.DocumentFile;

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
		
		System.out.println(io.getActiveDatabaseSize());
		
		if(io.getActiveDatabaseSize()!=size+count) {
			fail("Incorrect database size after inserts. Expected "+(size+count)+" but found "+io.getActiveDatabaseSize());
		}
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
		for(Document d : list) {
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
	public void testWriteDocumentFile() throws IOException{
		if(io.getDocumentFileNames(test2).size()!=0) {
			fail("found file already attached to the document");
		}
		
		String content = TestTools.getRandomString(100);
		String name = TestTools.getRandomString(10);
		DocumentFile df = new DocumentFile(test2.getID(), name, IOUtils.toInputStream(content), "stage");

		io.write(df);
		DocumentFile df2 = io.getDocumentFile(test2, name);

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
		Document d = io.getDocument(dbq);
		
		if(d.isEqual(test) || !d.isEqual(test2)) {
			fail("Incorrect document returned");
		}
		
	}

	@Test
	public void testGetAndTagDocumentDatabaseQueryString() {
		MemoryQuery mdq = new MemoryQuery();
		mdq.requireContentFieldExists("name");
		Document d = io.getAndTag(mdq, "tag");
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
		if (allDocs.contains(discarded_d.getID()) == false) {
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
