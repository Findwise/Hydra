package com.findwise.hydra.mongodb;

import java.util.Map;
import java.util.Map.Entry;

import com.findwise.hydra.DatabaseQuery;
import com.findwise.hydra.Document.Action;
import com.findwise.hydra.DocumentID;
import com.findwise.hydra.JsonException;
import com.findwise.hydra.SerializationUtils;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;


public class MongoQuery implements DatabaseQuery<MongoType> {

	private QueryBuilder qb;
	
	public MongoQuery() {
		qb = QueryBuilder.start();
	}
	
	public MongoQuery(String json) throws JsonException {
		this();
		fromJson(json);
	}
	
	public final DBObject toDBObject()
	{
		return qb.get();
	}
	
	private QueryBuilder putMetadataField(String s) {
		qb = qb.put(MongoDocument.METADATA_KEY+"."+s);
		return qb;
	}
	
	private QueryBuilder putContentField(String s) {
		qb = qb.put(MongoDocument.CONTENTS_KEY+"."+s);
		return qb;
	}
	
	public final void requireID(DocumentID<MongoType> o) {
		if(o==null) {
			return;
		}
		qb = qb.put(MongoDocument.MONGO_ID_KEY).is(o.getID());
	}

	@Override
	public final void requireContentFieldExists(String s) {
		qb = putContentField(s).exists(true);
	}

	@Override
	public final void requireContentFieldEquals(String s, Object o) {
		qb = putContentField(s).is(o);
	}

	@Override
	public final void requireContentFieldNotExists(String s) {
		qb = putContentField(s).exists(false);
	}

	@Override
	public final void requireContentFieldNotEquals(String s, Object o) {
		qb = putContentField(s).notEquals(o);
	}

	@Override
	public final void requireMetadataFieldExists(String s) {
		qb = putMetadataField(s).exists(true);
	}

	@Override
	public final void requireMetadataFieldEquals(String s, Object o) {
		qb = putMetadataField(s).is(o);
	}
	
	@Override
	public final void requireMetadataFieldNotExists(String s) {
		qb = putMetadataField(s).exists(false);
	}

	@Override
	public final void requireMetadataFieldNotEquals(String s, Object o) {
		qb = putMetadataField(s).notEquals(o);
	}
	
	public final String toString() {
		return qb.get().toString();
	}

	@Override
	public final void requireTouchedByStage(String stageName) {
		requireMetadataFieldExists("touched."+stageName);
	}

	@Override
	public final void requireNotTouchedByStage(String stageName) {
		requireMetadataFieldNotExists("touched."+stageName);
	}
	
	public final void requireFetchedByStage(String stageName) {
		requireMetadataFieldExists("fetched."+stageName);
	}

	public final void requireNotFetchedByStage(String stageName) {
		requireMetadataFieldNotExists("fetched."+stageName);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public final void fromJson(String json) throws JsonException {
		try {
			Map<String, Object> queryObject = (Map<String, Object>) SerializationUtils.fromJson(json);
			if(queryObject.containsKey("equals")) {
				for (Entry<String, Object> entry : ((Map<String, Object>) queryObject.get("equals")).entrySet()) {
					requireContentFieldEquals(entry.getKey(), entry.getValue());
				}
			}
			if(queryObject.containsKey("notEquals")) {
				for (Entry<String, Object> entry : ((Map<String, Object>) queryObject.get("notEquals")).entrySet()) {
					requireContentFieldNotEquals(entry.getKey(), entry.getValue());
				}
			}
			if(queryObject.containsKey("exists")) {
				for (Entry<String, Boolean> entry : ((Map<String, Boolean>) queryObject.get("exists")).entrySet()) {
					if (entry.getValue()) {
						requireContentFieldExists(entry.getKey());
					} else {
						requireContentFieldNotExists(entry.getKey());
					}
				}
			}
			if(queryObject.containsKey("touched")) {
				for (Entry<String, Boolean> entry : ((Map<String, Boolean>) queryObject.get("touched")).entrySet()) {
					if (entry.getValue()) {
						requireTouchedByStage(entry.getKey());
					} else {
						requireNotTouchedByStage(entry.getKey());
					}
				}
			}
			if(queryObject.containsKey("fetched")) {
				for (Entry<String, Boolean> entry : ((Map<String, Boolean>) queryObject.get("fetched")).entrySet()) {
					if (entry.getValue()) {
						requireFetchedByStage(entry.getKey());
					} else {
						requireNotFetchedByStage(entry.getKey());
					}
				}
			}
			if(queryObject.containsKey("action")) {
				requireAction(Action.valueOf((String)queryObject.get("action")));
			}
			
		} 
		catch(JsonParseException jse) {
			throw new JsonException(jse);
		}
	}
	
	@Override
	public String toJson() {
		Gson gson = new Gson();
		return gson.toJson(qb.get().toMap());
	}
	
	@Override
	public final void requireAction(Action action) {
		qb = qb.put(MongoDocument.ACTION_KEY).is(action.toString());
	}

}
