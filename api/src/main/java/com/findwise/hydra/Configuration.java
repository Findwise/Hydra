package com.findwise.hydra;

public interface Configuration {
	
	Object getParameter(String key);
	
	Object getParameter(String key, String defaultValue);
	
}
