package com.findwise.hydra;

import com.findwise.hydra.Document.Action;

public interface Query extends JsonDeserializer, JsonSerializer {
	String TOUCHED_KEY = "touched";
	
	void requireContentFieldExists(String fieldName);

	void requireContentFieldNotExists(String fieldName);
	
	void requireTouchedByStage(String stageName);
	
	void requireNotTouchedByStage(String stageName);
	
	void requireContentFieldEquals(String fieldName, Object o);
	
	void requireContentFieldNotEquals(String fieldName, Object o);
	
	void requireAction(Action a);
}
