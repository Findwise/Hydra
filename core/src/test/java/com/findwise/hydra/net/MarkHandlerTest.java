package com.findwise.hydra.net;

import org.junit.Before;
import org.junit.Test;

import com.findwise.hydra.Document.Status;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.LocalQuery;
import com.findwise.hydra.local.RemotePipeline;
import com.findwise.hydra.memorydb.MemoryConnector;
import com.findwise.hydra.memorydb.MemoryDocument;
import com.findwise.hydra.memorydb.MemoryType;

import static org.junit.Assert.fail;

public class MarkHandlerTest {
	private MemoryConnector mc;
	private RESTServer server;
	private HttpRESTHandler<MemoryType> handler;
	
	@Before
	public void setUp() {
		mc = new MemoryConnector();
		handler = new HttpRESTHandler<MemoryType>(mc);
		server = RESTServer.getNewStartedRESTServer(20000, handler);
	}
	
	@Test
	public void testMarkPersistance() throws Exception {
		RemotePipeline rp = new RemotePipeline("localhost", server.getPort(), "x");
		
		LocalDocument doc = new LocalDocument();

		doc.putContentField("field", "value");
		doc.putContentField("field2", "value2");
		
		MemoryDocument d = (MemoryDocument)mc.convert(doc);
		mc.getDocumentWriter().insert(d);
		
		doc = rp.getDocument(new LocalQuery());
		
		doc.putContentField("field3", "value3");
		
		if(!rp.markProcessed(doc)) {
			fail("markProcessed returned false");
		}
		
		MemoryDocument doc2 = (MemoryDocument) mc.getDocumentReader().getDocumentById(d.getID(), true);
		
		for(String field : doc.getContentFields()) {
			if(!doc2.hasContentField(field)) {
				fail("Missing a field: "+field);
			}
			if(!doc.getContentField(field).equals(doc2.getContentField(field))) {
				fail("Content mismatch");
			}
		}
		
		if(!doc2.getStatus().equals(Status.PROCESSED)) {
			fail("No FAILED status on the document");
		}
	}
}
