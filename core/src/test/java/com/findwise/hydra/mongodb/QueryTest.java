package com.findwise.hydra.mongodb;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.TestModule;
import com.findwise.hydra.mongodb.MongoConnector;
import com.findwise.hydra.mongodb.MongoDocument;
import com.findwise.hydra.mongodb.MongoQuery;
import com.findwise.hydra.common.Document;
import com.findwise.hydra.common.DocumentFile;
import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.local.LocalQuery;
import com.google.inject.Guice;

/**
 * Cross-test
 * @author joel.westberg
 *
 */
public class QueryTest {

	DatabaseConnector mdc;
	DatabaseDocument test;
	DatabaseDocument test2;
	DatabaseDocument random;
	File f;
	
	@Before
	public void setUp() throws Exception {
		mdc = Guice.createInjector(new TestModule("junit-QueryTest")).getInstance(MongoConnector.class);
		mdc.waitForWrites(true);
		
		test = new MongoDocument();
		test.putContentField("name", "test");
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
		
		try {
			mdc.connect();
		}
		catch (IOException e) {
			fail("IOException when establishing connection");
		}
		
		DatabaseDocument d;
		while((d = mdc.getDocumentReader().getDocument(new MongoQuery()))!=null) {
			mdc.getDocumentWriter().delete(d);
		}
		
		mdc.getDocumentWriter().insert(test);
		mdc.getDocumentWriter().insert(test2);
		mdc.getDocumentWriter().insert(random);
		
		DocumentFile df = new DocumentFile(test.getID(), f.getName(), new FileInputStream(f));
		
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
	}

	@Test
	public void testMongoDatabaseQuery() throws JsonException {
		LocalQuery lq = new LocalQuery();
		lq.requireTouchedByStage("test");
		lq.requireNotTouchedByStage("test2");
		lq.requireContentFieldExists("exists");
		MongoQuery q = new MongoQuery(lq.toJson());
		Document d = mdc.getDocumentReader().getDocument(q);
		if(d!=null) {
			fail("Expected no document to be returned");
		}
		lq = new LocalQuery();
		if(mdc.getDocumentReader().getDocument(new MongoQuery(lq.toJson()))==null) {
			fail("Expected to find a document");
		}
	}
	
	@Test
	public void testRequireID() {
		Document d = mdc.getDocumentReader().getDocument(new MongoQuery());
		MongoQuery mdq = new MongoQuery();
		mdq.requireID(d.getID());
		if(null==mdc.getDocumentReader().getDocument(mdq)) {
			fail("Did not find a document with expected ID");
		}
		if(!mdc.getDocumentReader().getDocument(mdq).isEqual(d)) {
			fail("Found wrong document! "+d.getID());
		}
	}

	@Test
	public void testRequireContentFieldExists() throws JsonException {
		LocalQuery q = new LocalQuery();
		q.requireContentFieldExists("number");
		List<DatabaseDocument> ds = mdc.getDocumentReader().getDocuments(new MongoQuery(q.toJson()), 3);
		if(ds.size()!=1) {
			fail("Received incorrect number of documents..");
		}
		q = new LocalQuery();
		q.requireContentFieldExists("name");
		ds = mdc.getDocumentReader().getDocuments(new MongoQuery(q.toJson()), 3);
		if(ds.size()!=2) {
			fail("Received incorrect number of documents..");
		}
	}
	
	@Test
	public void testRequireContentFieldNotExists() throws JsonException {
		LocalQuery q = new LocalQuery();
		q.requireContentFieldNotExists("number");
		List<DatabaseDocument> ds = mdc.getDocumentReader().getDocuments(new MongoQuery(q.toJson()), 3);
		if(ds.size()!=2) {
			fail("Received incorrect number of documents..");
		}
		q = new LocalQuery();
		q.requireContentFieldNotExists("name");
		ds = mdc.getDocumentReader().getDocuments(new MongoQuery(q.toJson()), 3);
		if(ds.size()!=1) {
			fail("Received incorrect number of documents..");
		}
	}

	@Test
	public void testRequireTouchedByStage() throws JsonException {
		LocalQuery q = new LocalQuery();
		q.requireTouchedByStage("xyz");
		List<DatabaseDocument> ds = mdc.getDocumentReader().getDocuments(new MongoQuery(q.toJson()), 3);
		if(ds.size()!=0) {
			fail("Got documents, shouldn't have.");
		}
		
		Document d = mdc.getDocumentWriter().getAndTag(new MongoQuery(), "xyz");
		if(d==null) {
			fail("Should have gotten a document back...");
		}
		mdc.getDocumentWriter().markTouched(d.getID(), "xyz");
		
		ds = mdc.getDocumentReader().getDocuments(new MongoQuery(q.toJson()), 3);
		if(ds.size()!=1) {
			fail("Received incorrect number of documents..");
		}
	}

	@Test
	public void testRequireNotTouchedByStage() throws JsonException {
		LocalQuery q = new LocalQuery();
		q.requireNotTouchedByStage("xyz");
		List<DatabaseDocument> ds = mdc.getDocumentReader().getDocuments(new MongoQuery(q.toJson()), 3);
		if(ds.size()!=3) {
			fail("Received incorrect number of documents..");
		}
		
		Document d = mdc.getDocumentWriter().getAndTag(new MongoQuery(), "xyz");
		if(d==null) {
			fail("Should have gotten a document back...");
		}
		mdc.getDocumentWriter().markTouched(d.getID(), "xyz");
		
		ds = mdc.getDocumentReader().getDocuments(new MongoQuery(q.toJson()), 3);
		if(ds.size()!=2) {
			fail("Received incorrect number of documents..");
		}
	}

	@Test
	public void testFromJSON() throws JsonException {
		LocalQuery q = new LocalQuery();
		MongoQuery mdq = new MongoQuery();
		mdq.fromJson(q.toJson());
		if(mdq.toDBObject().keySet().size()!=0) {
			fail("Expected query to be empty");
		}
		
		q.requireContentFieldExists("name");
		mdq.fromJson(q.toJson());
		if(mdq.toDBObject().keySet().size()!=1) {
			fail("Expected query to have one value");
		}
		List<DatabaseDocument> list = mdc.getDocumentReader().getDocuments(mdq, 142);
		if(list.size()!=2) {
			fail("Expected to find two documents");
		}
		for(Document d : list) {
			if(!d.hasContentField("name")) {
				fail("Fetched document is missing content field name");
			}
		}
	}

}
