package com.findwise.hydra.mongodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.findwise.hydra.DatabaseQuery;
import com.findwise.hydra.Document.Action;
import com.findwise.hydra.DocumentID;
import com.findwise.hydra.JsonException;
import com.findwise.hydra.SerializationUtils;
import com.findwise.hydra.local.LocalQuery;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

public class MongoQuery implements DatabaseQuery<MongoType> {

	private QueryBuilder qb;
	
	private List<String> touchedBy;
	private List<String> notTouchedBy;
	private List<String> fetchedBy;
	private List<String> notFetchedBy;
	private List<String> metadataExists;
	private List<String> metadataNotExists;
	private Map<String, Object> metadataNotEquals;
	private Map<String, Object> metadataEquals;
	private Map<String, List<Object>> metadataNotContains;
	private Map<String, List<Object>> metadataContains;
	
	
	private LocalQuery lq;
	
	private DocumentID<MongoType> id;
	private Action action;
	
	public MongoQuery() {
		qb = QueryBuilder.start();
		touchedBy = new ArrayList<String>();
		notTouchedBy = new ArrayList<String>();
		fetchedBy = new ArrayList<String>();
		notFetchedBy = new ArrayList<String>();
		metadataExists = new ArrayList<String>();
		metadataNotExists = new ArrayList<String>();
		metadataEquals = new HashMap<String, Object>();
		metadataNotEquals = new HashMap<String, Object>();
		metadataContains = new HashMap<String, List<Object>>();
		metadataNotContains = new HashMap<String, List<Object>>();
		lq = new LocalQuery();
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
		id = o;
	}

	@Override
	public final void requireContentFieldExists(String s) {
		qb = putContentField(s).exists(true);
		lq.requireContentFieldExists(s);
	}

	@Override
	public final void requireContentFieldEquals(String s, Object o) {
		qb = putContentField(s).is(o);
		lq.requireContentFieldEquals(s, o);
	}

	@Override
	public final void requireContentFieldNotExists(String s) {
		qb = putContentField(s).exists(false);
		lq.requireContentFieldNotExists(s);
	}

	@Override
	public final void requireContentFieldNotEquals(String s, Object o) {
		qb = putContentField(s).notEquals(o);
		lq.requireContentFieldNotEquals(s, o);
	}

	@Override
	public final void requireMetadataFieldExists(String s) {
		requireMetadataFieldExists(s, true);
	}
	
	private void requireMetadataFieldExists(String s, boolean addToList) {
		qb = putMetadataField(s).exists(true);
		if(addToList) {
			metadataExists.add(s);
		}
	}
	
	@Override
	public final void requireMetadataFieldNotExists(String s) {
		requireMetadataFieldNotExists(s, true);
	}
	
	private void requireMetadataFieldNotExists(String s, boolean addToList) {
		qb = putMetadataField(s).exists(false);
		if(addToList) {
			metadataNotExists.add(s);
		}
	}

	@Override
	public final void requireMetadataFieldEquals(String s, Object o) {
		qb = putMetadataField(s).is(o);
		getMetadataEquals().put(s, o);
	}

	@Override
	public final void requireMetadataFieldNotEquals(String s, Object o) {
		qb = putMetadataField(s).notEquals(o);
		getMetadataNotEquals().put(s, o);
	}
	
	public final String toString() {
		return qb.get().toString();
	}

	@Override
	public final void requireTouchedByStage(String stageName) {
		requireMetadataFieldExists(MongoDocument.TOUCHED_METADATA_TAG + "." + stageName, false);
		touchedBy.add(stageName);
	}

	@Override
	public final void requireNotTouchedByStage(String stageName) {
		requireMetadataFieldNotExists(MongoDocument.TOUCHED_METADATA_TAG + "." + stageName, false);
		notTouchedBy.add(stageName);
	}
	
	public final void requireFetchedByStage(String stageName) {
		requireMetadataFieldContains(MongoDocument.MONGO_FETCHED_METADATA_TAG_LIST, stageName);
		fetchedBy.add(stageName);
	}

	public final void requireNotFetchedByStage(String stageName) {
		requireMetadataFieldNotContains(MongoDocument.MONGO_FETCHED_METADATA_TAG_LIST, stageName);
		notFetchedBy.add(stageName);
	}

	public final void requireMetadataFieldContains(String s, Object o) {
		List<Object> l;
		if (metadataContains.containsKey(s)) {
			l = metadataContains.get(s);
		} else {
			l = new ArrayList<Object>();
		}
		l.add(o);
		requireMetadataFieldContains(s, l);
	}

	public final void requireMetadataFieldContains(String s, List<Object> l) {
		qb = putMetadataField(s).all(l);
		metadataContains.put(s, l);
	}

	public final void requireMetadataFieldNotContains(String s, Object o) {
		List<Object> l;
		if (metadataNotContains.containsKey(s)) {
			l = metadataNotContains.get(s);
		} else {
			l = new ArrayList<Object>();
		}
		l.add(o);
		requireMetadataFieldNotContains(s, l);
	}

	public final void requireMetadataFieldNotContains(String s, List<Object> l) {
		qb = putMetadataField(s).notIn(l);
		metadataNotContains.put(s, l);
	}
	
	public final DocumentID<MongoType> getRequiredID() {
		return id;
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
		this.action = action;
	}

	public Map<String, Object> getContentsEquals() {
		return lq.getContentsEquals();
	}
	
	public Map<String, Object> getContentsNotEquals() {
		return lq.getContentNotEquals();
	}

	public List<String> getMetadataExists() {
		return metadataExists;
	}

	public List<String> getMetadataNotExists() {
		return metadataNotExists;
	}

	public List<String> getContentsNotExists() {
		List<String> list = new ArrayList<String>();
		
		for(Map.Entry<String, Boolean> e : lq.getContentsExists().entrySet()) {
			if(!e.getValue()) {
				list.add(e.getKey());
			}
		}
		
		return list;
	}

	public List<String> getContentsExists() {
		List<String> list = new ArrayList<String>();
		
		for(Map.Entry<String, Boolean> e : lq.getContentsExists().entrySet()) {
			if(e.getValue()) {
				list.add(e.getKey());
			}
		}
		
		return list;
	}
	
	public final List<String> getTouchedBy() {
		return touchedBy;
	}
	
	public final List<String> getNotTouchedBy() {
		return notTouchedBy;
	}
	
	public final List<String> getFetchedBy() {
		return fetchedBy;
	}
	
	public final List<String> getNotFetchedBy() {
		return notFetchedBy;
	}
	
	public final Action getAction() {
		return action;
	}

	public Map<String, Object> getMetadataEquals() {
		return metadataEquals;
	}

	public Map<String, Object> getMetadataNotEquals() {
		return metadataNotEquals;
	}

}
