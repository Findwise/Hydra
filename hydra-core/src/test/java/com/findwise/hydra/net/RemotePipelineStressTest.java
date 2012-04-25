package com.findwise.hydra.net;

import static org.junit.Assert.*;

import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.NodeMaster;
import com.findwise.hydra.local.LocalQuery;
import com.findwise.hydra.local.RemotePipeline;
import com.findwise.hydra.mongodb.MongoDocument;
import com.findwise.hydra.TestModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class RemotePipelineStressTest {
	private NodeMaster nm;
	private static RESTServer server;
	private static Injector inj;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
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
		DatabaseConnector dbc = nm.getDatabaseConnector();
		dbc.getDocumentWriter().deleteAll();
	}

	@After
	public void tearDown() throws Exception {
		DatabaseConnector dbc = nm.getDatabaseConnector();
		dbc.getDocumentWriter().deleteAll();
	}
	
	@Test
	public void testStressGetDocument() throws Exception {
		int count = 10000;
		RemotePipeline rp1 = new RemotePipeline("127.0.0.1", server.getPort(), "stage1");

		
		for(int i=0; i<count; i++) {
			MongoDocument d = new MongoDocument();
			d.putContentField(getRandomString(5), getRandomString(20));
			nm.getDatabaseConnector().getDocumentWriter().insert(d);
		}
		
		for(int i=0; i<count; i++) {
			if(rp1.getDocument(new LocalQuery())==null) {
				fail("Should not have been null...");
			}
		}
		
		if(rp1.getDocument(new LocalQuery())!=null) {
			fail("All documents should have been seen already!");
		}
		
		RemotePipeline rp2 = new RemotePipeline("127.0.0.1", server.getPort(), "stage2");
		for(int i=0; i<count; i++) {
			if(rp2.getDocument(new LocalQuery())==null) {
				fail("Should not have been null...");
			}
		}
		if(rp2.getDocument(new LocalQuery())!=null) {
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
