package com.findwise.hydra.mongodb;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.DatabaseQuery;
import com.findwise.hydra.TestModule;
import com.findwise.hydra.common.Document;
import com.findwise.hydra.common.DocumentFile;
import com.findwise.hydra.common.Document.Status;
import com.google.inject.Guice;
import com.mongodb.Mongo;

public class MongoConnectorTest {
	MongoConnector mdc;
	MongoDocument test;
	MongoDocument test2;
	MongoDocument random;
	File f;

	private void createAndConnect() throws Exception {
		mdc = Guice.createInjector(new TestModule("junit-MongoConnectorTest")).getInstance(MongoConnector.class);
		
		mdc.waitForWrites(true);
		
		mdc.connect();
	}
	
	@Before
	public void setUp() throws Exception {
		createAndConnect();
		
		test = new MongoDocument();
		test.putContentField("name", "test");
		test.putContentField("number", 1);
		test.putMetadataField("date", new Date());
		
		test2 = new MongoDocument();
		test2.putContentField("name", "test");
		test2.putContentField("number", 2);
		test2.putMetadataField("date", new Date());
		
		f = new File("test1234.txt");
		if(f.exists()) {
			fail("Failing setup");
		}
		
		FileWriter fw = new FileWriter(f);
		
		fw.write("This is a file @"+System.currentTimeMillis());
		fw.close();
		
		random = new MongoDocument();
		
		mdc.getDocumentWriter().deleteAll();
		
		mdc.getDocumentWriter().insert(test);
		mdc.getDocumentWriter().insert(test2);
		mdc.getDocumentWriter().insert(random);
		
		DocumentFile df = new DocumentFile(test.getID(), f.getName(), new FileInputStream(f), "setup");
		mdc.getDocumentWriter().write(df);	
	}

	@After
	public void tearDown() throws Exception {
		boolean successfulDelete = f.delete();
		for (int maxTries = 10; !successfulDelete && maxTries > 0; maxTries--) {
			System.gc();
			Thread.sleep(300);
			successfulDelete = f.delete();
		}
		if (!successfulDelete) {
			fail("TearDown failed to delete: " + f.getAbsolutePath());
		}

		mdc.getDocumentWriter().deleteAll();
	}
	
	@AfterClass
	public static void tearDownClass() throws Exception {
		new Mongo().getDB("junit-MongoConnectorTest").dropDatabase();
	}

	@Test
	public void testInsertDocument() {
		MongoDocument md = new MongoDocument();
		md.putContentField("name", "blahonga");
		
		MongoQuery mdq = new MongoQuery();
		mdq.requireContentFieldEquals("name", "blahonga");
		
		if(mdc.getDocumentReader().getDocument(mdq)!=null) {
			fail("Document already exists in database");
		}
		
		mdc.getDocumentWriter().insert(md);
		
		Document d = mdc.getDocumentReader().getDocument(mdq);
		if(d==null) {
			fail("No document in test database");
		}
	}

	@Test
	public void testUpdateDocument() {
		String field = "xyz";
		String content = "zyx";
		MongoQuery mdq = new MongoQuery();
		mdq.requireContentFieldEquals("name", "test");
		DatabaseDocument<MongoType> d = mdc.getDocumentReader().getDocument(mdq);
		d.putContentField(field, content);
		mdc.getDocumentWriter().update(d);
		mdq = new MongoQuery();
		mdq.requireContentFieldEquals(field, content);
		d = mdc.getDocumentReader().getDocument(mdq);
		
		if(!d.getContentField("xyz").equals("zyx")) {
			fail("Wrong data in updated field");
		}
	}

	@Test
	public void testGetDocuments() {
		MongoQuery mdq = new MongoQuery();
		List<DatabaseDocument<MongoType>> list = mdc.getDocumentReader().getDocuments(mdq, 3);
		if(list.size()!=3) {
			fail("Did not return all documents. Expected 3, found "+list.size());
		}
		boolean test1 = false, test2=false, random=false;
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
			else {
				random = true;
			}
		}
		if (!test1 || !test2 || !random) {
			fail("Not all three documents were found");
		}
		
		mdq.requireContentFieldEquals("name", "test");
		list = mdc.getDocumentReader().getDocuments(mdq, 3);
		if(list.size()!=2) {
			fail("Wrong number of documents returned. Expected 2, found "+list.size());
		}
		
		mdq.requireContentFieldEquals("number", 2);
		list = mdc.getDocumentReader().getDocuments(mdq, 1);
		if(list.size()!=1) {
			fail("Wrong number of documents returned. Expected 1, found "+list.size());
		}
	}

	@Test
	public void testWriteDocumentFile() throws IOException{
		if(mdc.getDocumentReader().getDocumentFileNames(test2).size()!=0) {
			fail("Document already had files");
		}
		
		DocumentFile df = new DocumentFile(test2.getID(), f.getName(), new FileInputStream(f), "stage");

		mdc.getDocumentWriter().write(df);
		DocumentFile df2 = mdc.getDocumentReader().getDocumentFile(test2, f.getName());

		
		BufferedReader fr = new BufferedReader(new FileReader(f));
		BufferedReader fxr = new BufferedReader(new InputStreamReader(df2.getStream()));
		try {
			if (!fr.readLine().equals(fxr.readLine())) {
				fail("Content mismatch between saved file and loaded file");
			}
		}
		finally {
			fr.close();
			fxr.close();
		}
	}

	@Test
	public void testGetDocumentFile() throws IOException{
		DocumentFile fx = mdc.getDocumentReader().getDocumentFile(test, f.getName());

		if(!f.getName().equals(fx.getFileName())) {
			fail("Couldn't find document file");
		}

		BufferedReader fr = new BufferedReader(new FileReader(f));
		BufferedReader fxr = new BufferedReader(new InputStreamReader(fx.getStream()));
		try {
			if (!fr.readLine().equals(fxr.readLine())) {
				fail("Content mismatch between saved file and written file");
			}
		}
		finally {
			fr.close();
			fxr.close();
		}
		
	}
	
	@Test
	public void testGetDocumentDatabaseQuery() {
		DatabaseQuery<MongoType> dbq = new MongoQuery();
		dbq.requireContentFieldEquals("name", "test");
		dbq.requireContentFieldEquals("number", 2);
		Document d = mdc.getDocumentReader().getDocument(dbq);
		
		if(d.isEqual(test) || !d.isEqual(test2)) {
			fail("Incorrect document returned");
		}
		
	}

	@Test
	public void testGetAndTagDocumentDatabaseQueryString() {
		MongoQuery mdq = new MongoQuery();
		mdq.requireContentFieldExists("name");
		Document d = mdc.getDocumentWriter().getAndTag(mdq, "tag");
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
		mdc.getDocumentWriter().getAndTag(new MongoQuery(), "tag");
		mdc.getDocumentWriter().getAndTag(new MongoQuery(), "tag");
		
		d = mdc.getDocumentWriter().getAndTag(new MongoQuery(), "tag");
		if(d!=null) {
			fail("Expected all documents to be tagged, but found "+d);
		}
	}

	@Test
	public void testDelete() {
		MongoQuery query = new MongoQuery();
		query.requireContentFieldNotEquals("name", "test");
		MongoDocument d = (MongoDocument) mdc.getDocumentReader().getDocument(query);
		Object id = d.getID();
		mdc.getDocumentWriter().delete(d);
		query.requireID(id);
		if(mdc.getDocumentReader().getDocument(query)!=null) {
			fail("Document failed to be deleted.");
		}
	}
	
	@Test
	public void testDiscardDocument() {		
		DatabaseDocument<MongoType> discarded_d = mdc.getDocumentWriter().getAndTag(new MongoQuery(), "DiscardedTag");
		
		DatabaseDocument<MongoType> d;
		List<Object> allDocs = new ArrayList<Object>();
		while ((d = mdc.getDocumentWriter().getAndTag(new MongoQuery(), "testRet")) != null) {
			allDocs.add(d.getID());
		}
		if (allDocs.contains(discarded_d.getID()) == false) {
			fail("Discarded document should still be there since it's not yet discarded");
		}
		
		mdc.getDocumentWriter().markDiscarded(discarded_d, "test_stage");
		
		while ((d = mdc.getDocumentWriter().getAndTag(new MongoQuery(), "testGetNonDiscarded")) != null) {
			if(discarded_d.getID() == d.getID()) {
				fail("Discarded document was retrieved after discard");
			}
		}
		
		DatabaseDocument<MongoType> old = mdc.getDocumentReader().getDocumentById(discarded_d.getID(), true);
		if(old==null) {
			fail("Failed to find the document in the 'old' database");
		}
		
		if(old.getStatus()!=Status.DISCARDED) {
			fail("No discarded flag on the document");
		}
	}
	
	@Test
	public void testFailedDocument() {		
		DatabaseDocument<MongoType> failed = mdc.getDocumentWriter().getAndTag(new MongoQuery(), "failedTag");
		
		mdc.getDocumentWriter().markFailed(failed, "test_stage");
		
		if(mdc.getDocumentReader().getDocumentById(failed.getID())!=null) {
			fail("Failed document was retrieved after discard");
		}
		
		DatabaseDocument<MongoType> old = mdc.getDocumentReader().getDocumentById(failed.getID(), true);
		if(old==null) {
			fail("Failed to find the document in the 'old' database");
		}
		
		if(old.getStatus()!=Status.FAILED) {
			fail("No failed flag on the document");
		}
	}
	
	@Test
	public void testPendingDocument() {		
		DatabaseDocument<MongoType> pending = mdc.getDocumentWriter().getAndTag(new MongoQuery(), "pending");
		
		mdc.getDocumentWriter().markPending(pending, "test_stage");
		
		pending = mdc.getDocumentWriter().getDocumentById(pending.getID());
		
		if(pending.getStatus()!=Status.PENDING) {
			fail("No PENDING flag on the document");
		}
	}
	
	@Test
	public void testProcessedDocument() {		
		DatabaseDocument<MongoType> processed = mdc.getDocumentWriter().getAndTag(new MongoQuery(), "processed");
		
		mdc.getDocumentWriter().markProcessed(processed, "test_stage");
		
		processed = mdc.getDocumentWriter().getDocumentById(processed.getID(), true);
		
		if(processed.getStatus()!=Status.PROCESSED) {
			fail("No PROCESSED flag on the document");
		}
	}
	
	@Test
	public void testActiveDatabaseSize() {
		if(mdc.getDocumentReader().getActiveDatabaseSize() != 3) {
			fail("Not the correct active database size. Expected 3 got: "+mdc.getDocumentReader().getActiveDatabaseSize());
		}
		List<DatabaseDocument<MongoType>> list = mdc.getDocumentReader().getDocuments(new MongoQuery(), 2);
		mdc.getDocumentWriter().markProcessed(list.get(0), "x");
		mdc.getDocumentWriter().markDiscarded(list.get(1), "x");
		if(mdc.getDocumentReader().getActiveDatabaseSize() != 1) {
			fail("Not the correct active database size after processed and discard, expected 1 found " + mdc.getDocumentReader().getActiveDatabaseSize());
		}
	}
	

}
