package com.findwise.hydra.common;

import com.google.gson.JsonParseException;

/**
 * Class for encapsulating a JsonParseException in a non-runtime exception.
 * 
 * @author joel.westberg
 */
public final class JsonException extends Exception {
	private static final long serialVersionUID = -3556367227200786526L;
	
	public JsonException(JsonParseException t) {
		super(t);
	}
	
	public String getMessage() {
		Throwable t = getCause();
		while(t.getCause()!=null) {
			t = t.getCause();
		}
		return t.getMessage();
	}
}
