package com.findwise.hydra.net;

import static org.junit.Assert.*;

import java.io.IOException;
import org.junit.Test;

import com.findwise.hydra.MapConfiguration;
import com.findwise.hydra.TestModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class RESTServerTest {

	@Test
	public void testBlockingStart() throws IOException, InterruptedException {
		Injector inj =  Guice.createInjector(new TestModule("jUnit-RESTServerTest"));
		int port = inj.getInstance(MapConfiguration.class).getRestPort();
		RESTServer server1 = inj.getInstance(RESTServer.class);
		
		if(!server1.blockingStart()) {
			server1 = RESTServer.getNewStartedRESTServer(inj);
		}
		System.out.println("Started server 1 on port "+port);
		RESTServer server2 = inj.getInstance(RESTServer.class);
		if(server2.blockingStart()) {
			System.out.println("We are failing on port "+server2.getPort());
			Thread.sleep(1000);
			System.out.println("1 alive: "+server1.isAlive());
			System.out.println("2 alive: "+server2.isAlive());
			System.out.println("1 hasError: "+server1.hasError());
			System.out.println("2 hasError: "+server2.hasError());
			System.out.println("1 getError: "+server1.getError());
			System.out.println("2 getError: "+server2.getError());
			System.out.println("1 isExecuting: "+server1.isExecuting());
			System.out.println("2 isExecuting: "+server2.isExecuting());
			
			fail("blockingStart() returned true when port should already be taken");
		}
		server2 = RESTServer.getNewStartedRESTServer(inj);
		System.out.println("Restarted on port "+server2.getPort());
		server2 = RESTServer.getNewStartedRESTServer(inj);
		System.out.println("Restarted on port "+server2.getPort());
		server1.shutdown();
		server2.shutdown();
	}

	@Test
	public void testShutdown() throws IOException, InterruptedException {
		Injector inj =  Guice.createInjector(new TestModule("jUnit-RESTServerTest"));
		RESTServer server = RESTServer.getNewStartedRESTServer(inj);
		server.shutdown();
		Thread.sleep(1000);
		if(server.isAlive()) {
			fail("Thread should be dead");
		}
		if(server.isExecuting()) {
			fail("IOReactor should not be executing");
		}
		if(server.hasError()) {
			server.getError().printStackTrace();
			fail("Server should not have error "+ server.getError().getMessage());
		}
		if(server.isWorking(System.currentTimeMillis(), 1000)) {
			fail("Server should not be working");
		}
	}

}
