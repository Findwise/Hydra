package com.findwise.hydra.mongodb;

import java.util.Map;

import com.findwise.hydra.DatabaseQuery;
import com.findwise.hydra.common.Document.Action;
import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.local.LocalQuery;
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
	
	@SuppressWarnings("unchecked")
	public final void requireID(Object o) {
		Object id;
		if(o instanceof Map) {
			id = MongoDocument.toObjectId((Map<String, Object>) o);
		}
		else {
			id = o;
		}
		
		qb = qb.put(MongoDocument.MONGO_ID_KEY).is(id);
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
	
	@Override
	public final void fromJson(String json) throws JsonException {
		LocalQuery lq = new LocalQuery();
		lq.fromJson(json);
		fromLocalQuery(lq);
	}
	
	@Override
	public final void requireAction(Action action) {
		qb = qb.put(MongoDocument.MONGO_ID_KEY).is(action.toString());
	}

	private void fromLocalQuery(LocalQuery local) {
		for(String field : local.getContentsExists().keySet()) {
			if(local.getContentsExists().get(field)) {
				requireContentFieldExists(field);
			}
			else {
				requireContentFieldNotExists(field);
			}
		}
		for(Map.Entry<String, Object> entry : local.getContentsEquals().entrySet()) {
			requireContentFieldEquals(entry.getKey(), entry.getValue());
		}
		for(String field : local.getTouched().keySet()) {
			if(local.getTouched().get(field)) {
				requireTouchedByStage(field);
			}
			else {
				requireNotTouchedByStage(field);
			}
		}
	}
}
