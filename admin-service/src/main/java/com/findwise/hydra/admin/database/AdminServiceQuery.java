package com.findwise.hydra.admin.database;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.findwise.hydra.DatabaseQuery;
import com.findwise.hydra.common.Document.Action;
import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.common.SerializationUtils;
import com.findwise.hydra.local.LocalQuery;
import com.google.gson.JsonParseException;


public class AdminServiceQuery extends LocalQuery implements DatabaseQuery<AdminServiceType> {

	Map<String, Boolean> requireMetadataFieldExists;
	
	Map<String, Object> requireMetadataFieldEquals, requireMetadataFieldNotEquals;
	
	List<Action> requireAction;

	private Map<String, Boolean> fetched = new HashMap<String, Boolean>();
	 
	public AdminServiceQuery() {
		requireMetadataFieldExists = new HashMap<String, Boolean>();
		
		requireMetadataFieldEquals = new HashMap<String, Object>();
		requireMetadataFieldNotEquals = new HashMap<String, Object>();
		
		requireAction = new ArrayList<Action>();
	}
	
	public AdminServiceQuery(String jsonQuery) {
		try {
			fromJson(jsonQuery);
		} catch (JsonException e) {
			e.printStackTrace();
		}
	}

	@Override
	public String toJson() {
		Map<String, Object> x = new HashMap<String, Object>();
		x.put("equals", getEquals());
		x.put("notEquals", getNotEquals());
		x.put("exists", getExists());
		x.put("touched", getTouched());
		x.put("fetched", fetched);
		
		if(getAction()!=null) {
			x.put("action", getAction().toString());
		}
		
		return SerializationUtils.toJson(x); 
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void fromJson(String json) throws JsonException{
		try {

			Map<String, Object> queryObject = (Map<String, Object>) SerializationUtils.fromJson(json);
			if(queryObject.containsKey("fetched")) {
				fetched = (Map<String, Boolean>) queryObject.get("fetched");
			}
			super.fromJson(json);
			
		} 
		catch(JsonParseException jse) {
			throw new JsonException(jse);
		}
	}
	
	@Override
	public void requireMetadataFieldEquals(String fieldName, Object o) {
		requireMetadataFieldEquals.put(fieldName, o);
	}

	@Override
	public void requireMetadataFieldNotEquals(String fieldName, Object o) {
		requireMetadataFieldNotEquals.put(fieldName, o);
	}

	@Override
	public void requireMetadataFieldExists(String fieldName) {
		requireMetadataFieldExists.put(fieldName, true);
	}

	@Override
	public void requireMetadataFieldNotExists(String fieldName) {
		requireMetadataFieldExists.put(fieldName, false);
	}

	@Override
	public void requireNotFetchedByStage(String tag) {
		fetched.put(tag, false);
	}
}
