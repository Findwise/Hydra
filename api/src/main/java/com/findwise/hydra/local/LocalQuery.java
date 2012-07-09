package com.findwise.hydra.local;

import java.util.HashMap;
import java.util.Map;

import com.findwise.hydra.common.Document.Action;
import com.findwise.hydra.common.JsonDeserializer;
import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.common.Query;
import com.findwise.hydra.common.SerializationUtils;
import com.google.gson.JsonParseException;

public class LocalQuery implements Query, JsonDeserializer {
	private Map<String, Object> equalsMap;
	private Map<String, Boolean> existsMap;
	private Map<String, Boolean> touchedMap;
	private Action action = null;
	
	public LocalQuery() {
		equalsMap = new HashMap<String, Object>();
		existsMap = new HashMap<String, Boolean>();
		touchedMap = new HashMap<String, Boolean>();
	}
	
	public LocalQuery(String json) throws JsonException {
		this();
		fromJson(json);
	}
	
	
	public Map<String, Boolean> getContentsExists() {
		return existsMap;
	}
	
	public Map<String, Boolean> getTouched() {
		return touchedMap;
	}
	
	public Map<String, Object> getContentsEquals() {
		return equalsMap;
	}
	
	public Action getAction() {
		return action;
	}
	
	@Override
	public void requireContentFieldExists(String fieldName) {
		getContentsExists().put(fieldName, true);
	}

	@Override
	public void requireContentFieldNotExists(String fieldName) {
		getContentsExists().put(fieldName, false);
	}

	@Override
	public void requireContentFieldEquals(String fieldName, Object o) {
		getContentsEquals().put(fieldName, o);
	}

	@Override
	public void requireTouchedByStage(String stageName) {
		getTouched().put(stageName, true);
	}

	@Override
	public void requireNotTouchedByStage(String stageName) {
		getTouched().put(stageName, false);
	}
	
	@Override
	public void requireAction(Action action) {
		this.action = action;
	}

	public String toJson() {
		Map<String, Object> x = new HashMap<String, Object>();
		x.put("equalsMap", equalsMap);
		x.put("existsMap", existsMap);
		x.put("touchedMap", touchedMap);
		
		if(action!=null) {
			x.put("action", action.toString());
		}
		
		return SerializationUtils.toJson(x); 
	}
	
	@SuppressWarnings({ "unchecked" })
	@Override
	public void fromJson(String json) throws JsonException{
		try {
			Map<String, Object> queryObject = (Map<String, Object>) SerializationUtils.fromJson(json);
			if(queryObject.containsKey("equalsMap")) {
				equalsMap = (Map<String, Object>) queryObject.get("equalsMap");
			}
			if(queryObject.containsKey("existsMap")) {
				existsMap = (Map<String, Boolean>) queryObject.get("existsMap");
			}
			if(queryObject.containsKey("touchedMap")) {
				touchedMap = (Map<String, Boolean>) queryObject.get("touchedMap");
			}
			if(queryObject.containsKey("action")) {
				action = Action.valueOf((String)queryObject.get("action"));
			}
			
		} 
		catch(JsonParseException jse) {
			throw new JsonException(jse);
		}
	}
	
	@Override
	public String toString() {
		return toJson();
	}
}
