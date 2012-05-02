package com.findwise.hydra;

public interface Configuration {
	final int POLLING_INTERVAL_DEFAULT = 1000;
	
	final String POLLING_INTERVAL_PARAM = "polling_interval";
	final String REST_PORT_PARAM = "rest_port";
	
	int getRestPort();
	
	Object getParameter(String key);
	
	Object getParameter(String key, String defaultValue);
	
}
