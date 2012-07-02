package com.findwise.hydra.net;

import static org.junit.Assert.fail;

import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.NodeMaster;
import com.findwise.hydra.TestModule;
import com.findwise.hydra.local.LocalQuery;
import com.findwise.hydra.local.RemotePipeline;
import com.findwise.hydra.mongodb.MongoConnector;
import com.findwise.hydra.mongodb.MongoDocument;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class RemotePipelineStressTest {
	private NodeMaster nm;
	private static RESTServer server;
	private static Injector inj;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		root.setLevel(Level.OFF);
		
		inj = Guice.createInjector(new TestModule("jUnit-RemotePipelineStressTest"));
		server = RESTServer.getNewStartedRESTServer(inj);
		
		NodeMaster nm = inj.getInstance(NodeMaster.class);
		if(!nm.isAlive()) {
			nm.blockingStart();
		
			nm.getDatabaseConnector().waitForWrites(true);
			nm.getDatabaseConnector().connect();
		}
	}
	
	@Before
	public void setUp() {
		nm = inj.getInstance(NodeMaster.class);
		DatabaseConnector<?> dbc = nm.getDatabaseConnector();
		dbc.getDocumentWriter().deleteAll();
	}

	@After
	public void tearDown() throws Exception {
		DatabaseConnector<?> dbc = nm.getDatabaseConnector();
		dbc.getDocumentWriter().deleteAll();
		System.out.println("Deleted everything\n");
	}

	@AfterClass
	public static void tearDownClass() throws Exception {
		NodeMaster nm = inj.getInstance(NodeMaster.class);
		if(!nm.isAlive()) {
			nm.blockingStart();
		
			nm.getDatabaseConnector().waitForWrites(true);
			nm.getDatabaseConnector().connect();
		}
		//((MongoConnector)nm.getDatabaseConnector()).getDB().dropDatabase();
	}
	
	@Test
	public void testStressGet100() throws Exception {
		insertAndGet(100);
	}

	@Test
	public void testStressGet1000() throws Exception {
		insertAndGet(1000);
	}

	@Test
	public void testStressGet10000() throws Exception {
		insertAndGet(10000);
	}
	
	public void insertAndGet(int count) throws Exception {
		System.out.println("Inserted "+count+" documents in "+insertDocuments(count)+" ms");
		stressGetDocument(count);
		System.out.println();
	}
	
	public long insertDocuments(int count) throws Exception {
		long start = System.currentTimeMillis();
		for(int i=0; i<count; i++) {
			MongoDocument d = new MongoDocument();
			d.putContentField(getRandomString(5), getRandomString(20));
			nm.getDatabaseConnector().getDocumentWriter().insert(d);
		}
		return System.currentTimeMillis()-start;
	}
	
	public void stressGetDocument(int count) throws Exception {
		RemotePipeline rp1 = new RemotePipeline("127.0.0.1", server.getPort(), "stage1");

		
		long start = System.currentTimeMillis();
		for(int i=0; i<count; i++) {
			if(rp1.getDocument(new LocalQuery())==null) {
				fail("Should not have been null...");
			}
		}
		

		
		if(rp1.getDocument(new LocalQuery())!=null) {
			fail("All documents should have been seen already!");
		}
		System.out.println("Saw "+count+" documents in "+(System.currentTimeMillis()-start)+" ms");
		start = System.currentTimeMillis();
		RemotePipeline rp2 = new RemotePipeline("127.0.0.1", server.getPort(), "stage2");
		for(int i=0; i<count; i++) {
			if(rp2.getDocument(new LocalQuery())==null) {
				fail("Should not have been null...");
			}
		}

		System.out.println("Saw "+count+" documents again in "+(System.currentTimeMillis()-start)+" ms");
		if(rp2.getDocument(new LocalQuery())!=null) {
			fail("All documents should have been seen already!");
		}
		
		start = System.currentTimeMillis();
		RemotePipeline rp3 = new RemotePipeline("127.0.0.1", server.getPort(), "stage3");
		for(int i=0; i<count; i++) {
			if(rp3.getDocument(new LocalQuery())==null) {
				fail("Should not have been null...");
			}
		}

		System.out.println("Saw "+count+" documents again (again) in "+(System.currentTimeMillis()-start)+" ms");
		if(rp3.getDocument(new LocalQuery())!=null) {
			fail("All documents should have been seen already!");
		}
	}
	
	private String getRandomString(int length) {
		char[] ca = new char[length];

		Random r = new Random(System.currentTimeMillis());

		for (int i = 0; i < length; i++) {
			ca[i] = (char) ('A' + r.nextInt(26));
		}

		return new String(ca);
	}
}
