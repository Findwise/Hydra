package com.findwise.hydra.common;

import com.findwise.hydra.common.Document.Action;

public interface Query extends JsonDeserializer {
	String TOUCHED_KEY = "touched";
	
	void requireContentFieldExists(String fieldName);

	void requireContentFieldNotExists(String fieldName);
	
	void requireTouchedByStage(String stageName);
	
	void requireNotTouchedByStage(String stageName);
	
	void requireContentFieldEquals(String fieldName, Object o);
	
	void requireAction(Action a);
}
