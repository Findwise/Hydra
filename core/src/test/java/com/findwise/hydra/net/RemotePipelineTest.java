package com.findwise.hydra.net;

import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.NodeMaster;
import com.findwise.hydra.TestModule;
import com.findwise.hydra.common.Document.Action;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.LocalQuery;
import com.findwise.hydra.local.RemotePipeline;
import com.findwise.hydra.mongodb.MongoDocument;
import com.findwise.hydra.mongodb.MongoDocumentIO;
import com.findwise.hydra.mongodb.MongoType;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class RemotePipelineTest {
	private NodeMaster nm;
	private MongoDocument test, test2;
	
	private static RESTServer server;
	private static Injector inj;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		inj =  Guice.createInjector(new TestModule("jUnit-RemotePipelineTest"));
		server = RESTServer.getNewStartedRESTServer(inj);
		
		NodeMaster nm = inj.getInstance(NodeMaster.class);
		if(!nm.isAlive()) {
			nm.blockingStart();
		
			nm.getDatabaseConnector().waitForWrites(true);
			nm.getDatabaseConnector().connect();
		}
	}
	
	@Before
	public void setUp() throws Exception {
		nm = inj.getInstance(NodeMaster.class);
		if(!nm.isAlive()) {
			fail("NodeMaster is not running (TEST FAILIURE)");
		}
		
		test = new MongoDocument();
		test.setAction(Action.ADD);
		test.putContentField("name", "test");
		test.putContentField("type", "awesome");
		test.putContentField("unique", true);
		
		test2 = new MongoDocument();
		test2.setAction(Action.ADD);
		test2.putContentField("name", "test2");
		test2.putContentField("type", "fabulous");
		
		nm.getDatabaseConnector().getDocumentWriter().deleteAll();

		nm.getDatabaseConnector().getDocumentWriter().insert(test);
		nm.getDatabaseConnector().getDocumentWriter().insert(test2);
	}

	@After
	public void tearDown() throws Exception {
		DatabaseConnector<MongoType> dbc = nm.getDatabaseConnector();
		dbc.getDocumentWriter().deleteAll();
	}
	
	@Test
	public void testPersistedAction() {
		RemotePipeline rp = new RemotePipeline("127.0.0.1", server.getPort(), "stage1");
		try {
			LocalDocument ld = rp.getDocument(new LocalQuery());
			if(ld.getAction()!=test.getAction()) {
				fail("Incorrect action. Got "+ld.getAction()+", should have been "+test.getAction());
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("getDocument threw exception");
		}
		
	}

	@Test
	public void testRemotePipeline() {
		RemotePipeline rp = new RemotePipeline("127.0.0.1", server.getPort(), "stage1");
		try {
			rp.getDocument(new LocalQuery());
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("getDocument threw exception");
		}
	}
	
	@AfterClass
	public static void teardownAfterClass() throws Exception {
		server.shutdown();
	}
	

	@Test
	public void testGetDocument() throws Exception {
		RemotePipeline rp1 = new RemotePipeline("127.0.0.1", server.getPort(), "stage1");
		LocalDocument d = rp1.getDocument(new LocalQuery());
		if(d==null) {
			fail("getDocument returned null");
		}
		LocalDocument d2 = rp1.getDocument(new LocalQuery());
		if(d2==null) {
			fail("getDocument returned null");
		}
		LocalDocument d3 = rp1.getDocument(new LocalQuery());
		if(d3!=null) {
			fail("Got document, but was expecting null. Both documents in DB should have been tagged by stage1 already. This was "+d3.getID()+", previous were "+d.getID()+" and "+d2.getID());
		}
		
		RemotePipeline rp2 = new RemotePipeline("127.0.0.1", server.getPort(), "stage2");
		LocalQuery lq = new LocalQuery();
		lq.requireContentFieldNotExists("name");
		
		if(null!=rp2.getDocument(lq)) {
			fail("Got document, but was expecting null. Both documents in db have a name");
		}
	}
	
	@Test
	public void testReleaseDocument() throws Exception {
		RemotePipeline rp1 = new RemotePipeline("127.0.0.1", server.getPort(), "stage1");
		RemotePipeline rp2 = new RemotePipeline("127.0.0.1", server.getPort(), "stage2");
		
		LocalQuery lq = new LocalQuery();
		lq.requireTouchedByStage("stage1");
		
		if(null!=rp2.getDocument(lq)) {
			fail("Got document, but was expecting null. No documents have been touched by stage1");
		}
		
		rp1.getDocument(new LocalQuery());
		rp1.releaseLastDocument();
		LocalDocument ld = rp2.getDocument(lq);
		if(null==ld) {
			fail("Was expecting a document, but got null. One document has been touched by stage1");
		}
		
		lq = new LocalQuery();
		lq.requireNotTouchedByStage("stage1");
		if(ld.isEqual(rp2.getDocument(lq))) {
			fail("Expected these two documents to not be equal, one has been touched by stage1, one hasn't.");
		}
	}

	@Test
	public void testSaveCurrentDocument() throws Exception {
		RemotePipeline rp1 = new RemotePipeline("127.0.0.1", server.getPort(), "stage1");
		RemotePipeline rp2 = new RemotePipeline("127.0.0.1", server.getPort(), "stage2");
		LocalDocument d = rp1.getDocument(new LocalQuery());
		d.putContentField("value", "newField");

		rp1.saveCurrentDocument();
		
		if(!d.isSynced()) {
			fail("Document should be in sync");
		}

		if(rp1.saveCurrentDocument()) {
			fail("Should not be any documents to save..");
		}
		LocalQuery lq = new LocalQuery();
		lq.requireTouchedByStage("stage1");
		if(null==rp2.getDocument(lq)) {
			fail("The document was not marked as touched properly by stage1");
		}
		rp2.saveCurrentDocument();
		
		if(null!=rp2.getDocument(lq)) {
			fail("Was not expecting to find another document there..");
		}
	}
	
	@Test
	public void testSave() throws Exception {
		RemotePipeline rp1 = new RemotePipeline("127.0.0.1", server.getPort(), "stage1");
		LocalDocument d1 = rp1.getDocument(new LocalQuery());
		LocalDocument d2 = rp1.getDocument(new LocalQuery());
		d1.putContentField("x", "y");
		d1.addError("s1", new NullPointerException("xyz"));
		rp1.save(d1);
		d2.putContentField("x2", "z");
		rp1.saveCurrentDocument();
		
		RemotePipeline rp2 = new RemotePipeline("127.0.0.1", server.getPort(), "stage2");
		LocalQuery query = new LocalQuery();
		query.requireContentFieldExists("x");
		LocalDocument x = rp2.getDocument(query);
		if(null==x) {
			fail("Out of sequence updated document was not found");
		}
		if(!x.hasErrors()) {
			fail("This document should have an error!");
		}
	
		query = new LocalQuery();
		query.requireContentFieldExists("x2");
		if(null==rp2.getDocument(query)) {
			fail("Current document was not saved properly after out of sequence update");
		}
		
		LocalDocument d = new LocalDocument();
		d.putContentField("aField", "aValue");
		
		if(rp2.save(d)) {
			fail("Save of this document should have failed, it has no id");
		}
	}
	
	@Test
	public void testSaveIncorrectId() throws Exception {
		RemotePipeline rp1 = new RemotePipeline("127.0.0.1", server.getPort(), "stage1");
		boolean res = rp1.save(new LocalDocument("{_id : 123}"));
		if(res) {
			fail("Save returned false even though it should fail -- that ID doesn't exist.");
		}
		
		LocalDocument ld = rp1.getDocument(new LocalQuery());
		res = rp1.save(new LocalDocument("{_id : "+ld.getID()+"}"));
		if(!res) {
			fail("Save returned false, but it should be ok");
		}
	}
	
	@Test
	public void testSaveFull() throws Exception {
		//Primary use case: Write an entirely new document to the database
		RemotePipeline rp1 = new RemotePipeline("127.0.0.1", server.getPort(), "inputNode");
		RemotePipeline rp2 = new RemotePipeline("127.0.0.1", server.getPort(), "stage");
		RemotePipeline rp3 = new RemotePipeline("127.0.0.1", server.getPort(), "stage2");
		LocalDocument ld = new LocalDocument();
		ld.putContentField("fieldName", "unique!");
		ld.putContentField("status", "new");
		ld.setAction(Action.DELETE);
		
		if(!rp1.saveFull(ld)) {
			fail("saveFull returned false");
		}

		if(ld.getID()==null) {
			fail("Expected to get an id added to my document");
		}
		
		if(!ld.isSynced()) {
			fail("Document should be in sync after a write");
		}

		LocalQuery q = new LocalQuery();
		q.requireContentFieldExists("fieldName");
		LocalDocument d = rp2.getDocument(q);
		if(null==d) {
			fail("Inserted document was not found by a stage.");
		}
		
		if(!d.getID().equals(ld.getID())) {
			fail("Retreived document had a different id");
		}
		
		if(d.getAction()!=Action.DELETE) {
			fail("Did not get the correct action. Was "+d.getAction()+" should have been "+Action.DELETE);
		}
		
		d.putContentField("x", "y");
		if(!rp1.saveFull(d)) {
			fail("saveFull returned false");
		}
		
		if(!d.getID().equals(ld.getID())) {
			fail("ID was changed on document d");
		}
		d = rp3.getDocument(q);
		if(!d.getID().equals(ld.getID())) {
			fail("ID of document was changed durinc update");
		}
		if(!d.hasContentField("x")) {
			fail("Did not find expected content field");
		}
		if(!d.getContentField("x").equals("y")) {
			fail("Content field had incorrect content");
		}
	}
	
	@Test
	public void testKeepLock() throws Exception {
		RemotePipeline rp1 = new RemotePipeline("127.0.0.1", server.getPort(), "stage1");
		RemotePipeline rp2 = new RemotePipeline("127.0.0.1", server.getPort(), "stage2");
		LocalDocument d = rp1.getDocument(new LocalQuery());
		d.putContentField("value", "newField");

		rp1.keepLock();
		rp1.saveCurrentDocument();
		
		if(!d.isSynced()) {
			fail("Document should be in sync");
		}

		LocalQuery lq = new LocalQuery();
		lq.requireTouchedByStage("stage1");
		if(null!=rp2.getDocument(lq)) {
			fail("The document should not have been marked as touched yet by stage1");
		}
		
		if(!rp1.saveCurrentDocument()) {
			fail("Document should be ok to save");
		}
		
		if(null==rp2.getDocument(lq)) {
			fail("The document should have been marked as touched by stage1 now");
		}
	}

	@Test
	public void testGetRecurring() throws Exception {
		RemotePipeline rp1 = new RemotePipeline("127.0.0.1", server.getPort(), "stage1");
		if(rp1.getDocument(new LocalQuery(), true)==null) {
			fail("getDocument returned null");
		}
		if(rp1.getDocument(new LocalQuery(), true)==null) {
			fail("getDocument returned null");
		}
		if(rp1.getDocument(new LocalQuery(), true)!=null) {
			fail("Got document, but was expecting null. Not enough time should have passed for the recurring getDocument to return a doc.");
		}
		
		Thread.sleep(MongoDocumentIO.DEFAULT_RECURRING_INTERVAL+1000);
		if(rp1.getDocument(new LocalQuery(), true)==null) {
			fail("Didn't get a document, the timeout should have been over by now.");
		}
	}
	
	@Test
	public void testMarkProcessed() throws Exception {
		RemotePipeline rp1 = new RemotePipeline("127.0.0.1", server.getPort(), "stage1");
		RemotePipeline rp2 = new RemotePipeline("127.0.0.1", server.getPort(), "outputNode");

		LocalDocument d = rp2.getDocument(new LocalQuery());
		if(!rp2.markProcessed(d)) {
			fail("markProcessed returned false");
		}
		
		if(rp1.getDocument(new LocalQuery())==null) {
			fail("getDocument returned null");
		}
		if(rp1.getDocument(new LocalQuery())!=null) {
			fail("getDocument returned a document, one document should have been touched already, the other processed.");
		}
	}
	
	@Test
	public void testMarkPending() throws Exception {
		RemotePipeline rp1 = new RemotePipeline("127.0.0.1", server.getPort(), "stage1");
		RemotePipeline rp2 = new RemotePipeline("127.0.0.1", server.getPort(), "outputNode");

		LocalDocument d = rp2.getDocument(new LocalQuery());
		if(!rp2.markPending(d)) {
			fail("markPending returned false");
		}
		
		if(rp1.getDocument(new LocalQuery())==null) {
			fail("getDocument returned null");
		}
		if(rp1.getDocument(new LocalQuery())!=null) {
			fail("getDocument returned a document, one document should have been touched already, the other pending.");
		}
	}
	
	@Test
	public void testMarkDiscarded() throws Exception {
		RemotePipeline rp = new RemotePipeline("127.0.0.1", server.getPort(), "testStage");
		RemotePipeline rpOut = new RemotePipeline("127.0.0.1", server.getPort(), "outStage");
		
		LocalQuery lq = new LocalQuery();
		LocalDocument testDoc = rp.getDocument(lq);
		if (testDoc == null) {
			fail("Should find at least one document.");
		}
		
		if (!rp.markDiscarded(testDoc)) {
			fail("markDiscarded returned false");
		}
		
		LocalDocument tmp;
		while ((tmp = rpOut.getDocument(lq)) != null) {
			if (tmp.getContentField("name").equals(testDoc.getContentField("name"))) {
				fail("Should not retrieve a document which has been discarded.");
			}	
		}
	}
	
	@Test
	public void testMarkFailed() throws Exception {
		RemotePipeline rp = new RemotePipeline("127.0.0.1", server.getPort(), "testStage");
		
		LocalQuery lq = new LocalQuery();
		LocalDocument testDoc = rp.getDocument(lq);
		if (testDoc == null) {
			fail("Should find at least one document.");
		}
		
		if (!rp.markFailed(testDoc)) {
			fail("markFailed returned false");
		}

		if(nm.getDatabaseConnector().getDocumentReader().getDocumentById(testDoc.getID())!=null) {
			fail("Document was found even though markFailed had been called");
		}
		
		testDoc = rp.getDocument(lq);
		if(!rp.markFailed(testDoc, new NullPointerException("message"))) {
			fail("markFailed(Doc, Throwable) returned false");
		}
		

		if(nm.getDatabaseConnector().getDocumentReader().getDocumentById(testDoc.getID())!=null) {
			fail("Document was found even though markFailed(Doc, Throwable) had been called");
		}
		
		DatabaseDocument<MongoType> dbdoc = nm.getDatabaseConnector().getDocumentReader().getDocumentById(testDoc.getID(), true);
		
		if(!dbdoc.hasErrors()) {
			fail("dbdocument had no errors");
		}
	}
	
}
