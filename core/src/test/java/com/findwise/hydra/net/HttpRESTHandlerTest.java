package com.findwise.hydra.net;

import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import com.findwise.hydra.local.RemotePipeline;
import com.findwise.hydra.memorydb.MemoryConnector;
import com.findwise.hydra.memorydb.MemoryType;

public class HttpRESTHandlerTest {

	private MemoryConnector mc;
	private RESTServer server;
	private HttpRESTHandler<MemoryType> restHandler;
	
	@Before
	public void setUp() {
		mc = new MemoryConnector();
		restHandler = new HttpRESTHandler<MemoryType>(mc);
		server = RESTServer.getNewStartedRESTServer(20000, restHandler);
	}
	
	@Test
	public void testSupportsAllUrls() throws IllegalArgumentException, IllegalAccessException {
		Field[] fields = RemotePipeline.class.getFields();
		for(Field f : fields) {
			int mod = f.getModifiers();
			if(Modifier.isFinal(mod) && f.getName().endsWith("_URL")) {
				if(!isSupported((String) f.get(null))) {
					fail("Unsupported URL found!");
				}
			}
		}
	}
	
	private boolean isSupported(String url) {
		for(String s : restHandler.getSupportedUrls()) {
			if(url.equals(s)) {
				return true;
			}
		}
		return false;
	}
	
	@Test
	public void testAccessRestrictions() throws InterruptedException {
		restHandler.setAllowedHosts(Arrays.asList(new String[] {"localhost","127.0.0.1"}));
		
		if(!server.isWorking(System.currentTimeMillis(), 200))
		{
			fail("Server should have been working, we are connecting through localhost...");
		}
		
		restHandler.setAllowedHosts(Arrays.asList(new String[] {"some_other_machine"}));
		
		if(server.isWorking(System.currentTimeMillis(), 200))
		{
			fail("Server should have been working, we are connecting through localhost...");
		}
		
		restHandler.setAllowedHosts(null);
		if(!server.isWorking(System.currentTimeMillis(), 200))
		{
			fail("Server should have been working, since allowed hosts is null");
		}
	}
}
