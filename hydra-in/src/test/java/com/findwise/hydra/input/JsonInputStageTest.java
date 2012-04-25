package com.findwise.hydra.input;

import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.findwise.tools.HttpConnection;

public class JsonInputStageTest {
	private String goodJsonObject;
	private String badJsonObject;
	
	private static HttpConnection httpConn;
	
	
	//TODO: fix tests
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		//inj =  Guice.createInjector(new TestModule("junit-JsonInputStageTest"));
		//server = RESTServer.getNewStartedRESTServer(inj);
        
		/*
		NodeMaster nm = inj.getInstance(NodeMaster.class);
		if(!nm.isAlive()) {
			nm.start();
			nm.getDatabaseConnector().waitForWrites(true);
		}
		*/
		
		//httpConn = new HttpConnection("127.0.0.1", port);
	}

	@AfterClass
	public static void teardownAfterClass() throws Exception {
		//server.shutdown();
	}

	@Before
	public void setUp() throws Exception {
		//goodJsonObject = "{\"contents\" : {\"test1\" : \"value1\"}}";
		//badJsonObject = "{\"contents\" {\"test2\" : \"value2\"}}";
	}
	
	@After
	public void tearDown() throws Exception {
	}
	
	@Ignore
	@Test
	public void testGoodJsonObject() throws IOException, HttpException {
		HttpResponse response = httpConn.post("/", goodJsonObject);
		int status = response.getStatusLine().getStatusCode();
		EntityUtils.consume(response.getEntity());
		if(status != HttpStatus.SC_OK) {
			fail("Got error response from JsonHandler");
		}
	}
	
	@Ignore
	@Test
	public void testBadJsonObject() throws IOException, HttpException {
		HttpResponse response = httpConn.post("/", badJsonObject);
		int status = response.getStatusLine().getStatusCode();
		EntityUtils.consume(response.getEntity());
		if(status != HttpStatus.SC_BAD_REQUEST) {
			fail("Did not get error 400 response from JsonHandler");
		}
	}
	
	@Ignore
	@Test
	public void testGoodAndBadJsonObject() throws IOException, HttpException {
		testGoodJsonObject();
		testBadJsonObject();
	}
}
