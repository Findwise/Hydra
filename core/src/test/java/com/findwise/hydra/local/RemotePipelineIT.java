package com.findwise.hydra.local;

import com.findwise.hydra.CachingDocumentNIO;
import com.findwise.hydra.ConfigurationFactory;
import com.findwise.hydra.CoreConfiguration;
import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.ShutdownHandler;
import com.findwise.hydra.Document.Action;
import com.findwise.hydra.DocumentFile;
import com.findwise.hydra.NodeMaster;
import com.findwise.hydra.NoopCache;
import com.findwise.hydra.Pipeline;
import com.findwise.hydra.mongodb.MongoConnector;
import com.findwise.hydra.mongodb.MongoDocument;
import com.findwise.hydra.mongodb.MongoDocumentID;
import com.findwise.hydra.mongodb.MongoType;
import com.findwise.hydra.net.HttpRESTHandler;
import com.findwise.hydra.net.RESTServer;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RemotePipelineIT {
	public static final String TEST_NAME = "jUnit-RemotePipelineTest";
	private static NodeMaster<MongoType> nm;
	private MongoDocument test, test2;
	private static MongoConnector dbc;
	
	private static RESTServer server;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		CoreConfiguration conf = ConfigurationFactory.getConfiguration(TEST_NAME);
		new MongoClient(new MongoClientURI(conf.getDatabaseUrl())).getDB(TEST_NAME).dropDatabase();
		dbc = new MongoConnector(conf);
		
		ShutdownHandler shutdownHandler = Mockito.mock(ShutdownHandler.class);
		
		Mockito.when(shutdownHandler.isShuttingDown()).thenReturn(false);
		
		nm = new NodeMaster<MongoType>(conf, new CachingDocumentNIO<MongoType>(dbc, new NoopCache<MongoType>(), false), new Pipeline(), shutdownHandler);
		if(!nm.isAlive()) {
			nm.blockingStart();
		
			dbc.waitForWrites(true);
			dbc.connect();
		}
		server = new RESTServer(conf, new HttpRESTHandler<MongoType>(dbc));
		server.start();
	}
	
	@Before
	public void setUp() throws Exception {
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
		
		dbc.getDocumentWriter().deleteAll();

		dbc.getDocumentWriter().insert(test);
		dbc.getDocumentWriter().insert(test2);
	}

	@After
	public void tearDown() throws Exception {
		dbc.getDocumentWriter().deleteAll();
	}
	
	@AfterClass
	public static void tearDownClass() throws Exception {
		CoreConfiguration conf = ConfigurationFactory.getConfiguration(TEST_NAME);
		new MongoClient(new MongoClientURI(conf.getDatabaseUrl())).getDB(TEST_NAME).dropDatabase();
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
	public void testSave() throws Exception {
		RemotePipeline rp1 = new RemotePipeline("127.0.0.1", server.getPort(), "stage1");
		LocalDocument d1 = rp1.getDocument(new LocalQuery());
		LocalDocument d2 = rp1.getDocument(new LocalQuery());
		d1.putContentField("x", "y");
		d1.addError("s1", new NullPointerException("xyz"));
		rp1.save(d1);
		d2.putContentField("x2", "z");
		rp1.save(d2);

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
	}
	
	@Test
	public void testSaveWithoutId() throws Exception {
		RemotePipeline rp2 = new RemotePipeline("127.0.0.1", server.getPort(), "stage2");
		
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
		res = rp1.save(new LocalDocument("{_id : "+ld.getID().getID()+"}"));
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
		
		MongoDocumentID mid = MongoDocumentID.getDocumentID(testDoc.getID().toJSON());
		if(dbc.getDocumentReader().getDocumentById(mid)!=null) {
			fail("Document was found even though markFailed had been called");
		}
		
		testDoc = rp.getDocument(lq);
		if(!rp.markFailed(testDoc, new NullPointerException("message"))) {
			fail("markFailed(Doc, Throwable) returned false");
		}
		
		mid = MongoDocumentID.getDocumentID(testDoc.getID().toJSON());
		if(dbc.getDocumentReader().getDocumentById(mid)!=null) {
			fail("Document was found even though markFailed(Doc, Throwable) had been called");
		}
		
		DatabaseDocument<MongoType> dbdoc = dbc.getDocumentReader().getDocumentById(mid, true);
		
		if(!dbdoc.hasErrors()) {
			fail("dbdocument had no errors");
		}
	}
	
	@Test
	public void testSaveFile() throws Exception {
		MongoDocument testDoc = new MongoDocument();
		dbc.getDocumentWriter().insert(testDoc);
		
		RemotePipeline rp = new RemotePipeline("localhost", server.getPort(), "stage");
		
		String content = "xäöåx";

		String fileName = "test.txt";
		DocumentFile<Local> df = new DocumentFile<Local>(new LocalDocument(testDoc.toJson()).getID(), fileName, IOUtils.toInputStream(content, "UTF-8"));
		df.setEncoding("UTF-8");
		
		if(!rp.saveFile(df)) {
			fail("RemotePipeline.saveFile() returned false");
		}
		
		DocumentFile<?> df2 = dbc.getDocumentReader().getDocumentFile(testDoc, fileName);
		
		if(df2==null) {
			fail("File was not properly saved");
		}
		
		if(!df2.getFileName().equals(fileName)) {
			fail("File had wrong file name");
		}
		
		String fc = IOUtils.toString(df2.getStream(), "UTF-8");
		if(!fc.equals(content)) {
			fail("File had wrong contents");
		}
	}
	
	@Test
	public void testRemoveField() throws Exception {
		dbc.getDocumentWriter().deleteAll();
		MongoDocument testDoc = new MongoDocument();
		testDoc.putContentField("field", "value");
		dbc.getDocumentWriter().insert(testDoc);
		RemotePipeline rp = new RemotePipeline("localhost", server.getPort(), "stage");
		
		LocalDocument ld = rp.getDocument(new LocalQuery());
		assertTrue(ld.hasContentField("field"));
		ld.removeContentField("field");
		
		rp.save(ld);
		
		rp = new RemotePipeline("localhost", server.getPort(), "stage2");
		ld = rp.getDocument(new LocalQuery());
		assertFalse(ld.hasContentField("field"));
	}
}
