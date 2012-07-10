package com.findwise.hydra.net;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.junit.Before;
import org.junit.Test;

import com.findwise.hydra.NodeMaster;
import com.findwise.hydra.TestModule;
import com.findwise.hydra.local.RemotePipeline;
import com.google.inject.Guice;

public class HttpRESTHandlerTest {
	private HttpRESTHandler<?> restHandler;
	
	@Before
	public void setUp() {
		restHandler = new HttpRESTHandler(Guice.createInjector(new TestModule("jUnit-HttpRESTHandlerTest")).getInstance(NodeMaster.class));
	}
	
	@Test
	public void supportsAllUrls() throws IllegalArgumentException, IllegalAccessException {
		Field[] fields = RemotePipeline.class.getFields();
		for(Field f : fields) {
			int mod = f.getModifiers();
			if(Modifier.isFinal(mod) && f.getName().endsWith("_URL")) {
				isSupported((String) f.get(null));
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
}
