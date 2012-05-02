package com.findwise.hydra.mongodb;

import static org.junit.Assert.fail;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.DocumentWriter;
import com.findwise.hydra.TestModule;
import com.google.inject.Guice;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class MongoDocumentIOTest {
	private MongoConnector mdc;

	private void createAndConnect() throws Exception {
	
		mdc = Guice.createInjector(new TestModule("junit-MongoDocumentIO")).getInstance(MongoConnector.class);
		
		mdc.waitForWrites(true);
		mdc.connect();
	}
	
	@Before
	public void setUp() throws Exception {
		createAndConnect();
		mdc.getDB().getCollection(MongoDocumentIO.OLD_DOCUMENT_COLLECTION).drop();
	}
	
	@AfterClass
	public static void tearDown() throws Exception {
		MongoDocumentIOTest test = new MongoDocumentIOTest();
		test.createAndConnect();
		test.mdc.getDB().dropDatabase();
	}
	
	@Test
	public void testPrepare() {
		DB db = mdc.getDB();
		
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
		DBCollection dbc = mdc.getDB().getCollection(MongoDocumentIO.OLD_DOCUMENT_COLLECTION);
		if(dbc.getStats().containsField("capped")) {
			return dbc.getStats().get("capped").equals(1);
		}
		return false;
	}
	
	@Test
	public void testConnectPrepare() throws Exception {
		mdc.getDB().dropDatabase();
		mdc.connect();
		if(!isCapped()) {
			fail("Collection was not capped on connect");
		}
	}
	
	@Test
	public void testRollover() throws Exception {
		DocumentWriter<MongoType> dw = mdc.getDocumentWriter();
		dw.prepare();
		for(int i=0; i<MongoPipelineStatus.DEFAULT_NUMBER_TO_KEEP; i++) {
			dw.insert(new MongoDocument());
			DatabaseDocument<MongoType> dd = dw.getAndTag(new MongoQuery(), "tag");
			dw.markProcessed(dd, "tag");
		}
		
		if(mdc.getDocumentReader().getActiveDatabaseSize()!=0) {
			fail("Still some active docs..");
		}
		if(mdc.getDocumentReader().getInactiveDatabaseSize()!=MongoPipelineStatus.DEFAULT_NUMBER_TO_KEEP) {
			fail("Incorrect number of old documents kept");
		}
		
		dw.insert(new MongoDocument());
		DatabaseDocument<MongoType> dd = dw.getAndTag(new MongoQuery(), "tag");
		dw.markProcessed(dd, "tag");
		
		if(mdc.getDocumentReader().getActiveDatabaseSize()!=0) {
			fail("Still some active docs..");
		}
		if(mdc.getDocumentReader().getInactiveDatabaseSize()!=MongoPipelineStatus.DEFAULT_NUMBER_TO_KEEP) {
			fail("Incorrect number of old documents kept: "+ mdc.getDocumentReader().getInactiveDatabaseSize());
		}
	}

}
