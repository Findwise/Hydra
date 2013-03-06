package com.findwise.hydra.local;

import java.util.HashMap;
import java.util.Map;

import com.findwise.hydra.JsonDeserializer;
import com.findwise.hydra.JsonException;
import com.findwise.hydra.Query;
import com.findwise.hydra.SerializationUtils;
import com.findwise.hydra.Document.Action;
import com.google.gson.JsonParseException;

public class LocalQuery implements Query, JsonDeserializer {
	private Map<String, Object> equals;
	private Map<String, Object> notEquals;
	private Map<String, Boolean> exists;
	private Map<String, Boolean> touched;
	private Action action = null;
	
	public LocalQuery() {
		equals = new HashMap<String, Object>();
		notEquals = new HashMap<String, Object>();
		exists = new HashMap<String, Boolean>();
		touched = new HashMap<String, Boolean>();
	}
	
	public LocalQuery(String json) throws JsonException {
		this();
		fromJson(json);
	}
	
	
	public Map<String, Boolean> getContentsExists() {
		return exists;
	}
	
	public Map<String, Boolean> getTouched() {
		return touched;
	}
	
	public Map<String, Object> getContentsEquals() {
		return equals;
	}
	
	public Map<String, Object> getEquals() {
		return equals;
	}

	public Map<String, Object> getContentNotEquals() {
		return notEquals;
	}
	
	public Map<String, Object> getNotEquals() {
		return notEquals;
	}

	public Map<String, Boolean> getExists() {
		return exists;
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
	public void requireContentFieldNotEquals(String fieldName, Object o) {
		getContentNotEquals().put(fieldName, o);
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
		x.put("equals", equals);
		x.put("notEquals", notEquals);
		x.put("exists", exists);
		x.put("touched", touched);
		
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
			if(queryObject.containsKey("equals")) {
				equals = (Map<String, Object>) queryObject.get("equals");
			}
			if(queryObject.containsKey("notEquals")) {
				notEquals = (Map<String, Object>) queryObject.get("notEquals");
			}
			if(queryObject.containsKey("exists")) {
				exists = (Map<String, Boolean>) queryObject.get("exists");
			}
			if(queryObject.containsKey("touched")) {
				touched = (Map<String, Boolean>) queryObject.get("touched");
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
