package com.findwise.hydra.mongodb;

import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.bson.BSONObject;

import com.findwise.hydra.AbstractPipelineStatus;
import com.mongodb.DBObject;

public class MongoPipelineStatus extends AbstractPipelineStatus<MongoType> implements DBObject {
	
	/**
	 * created defaults to NOW
	 * prepared defaults to false
	 */
	public MongoPipelineStatus() {
		setCreated(new Date());
		setPrepared(false);
		setDiscardedCount(0);
		setProcessedCount(0);
		setFailedCount(0);
	}
	
	@Override
	public boolean containsField(String arg0) {
		return getMap().containsKey(arg0);
	}

	@Override
	public boolean containsKey(String arg0) {
		return containsField(arg0);
	}

	@Override
	public Object get(String arg0) {
		return getMap().get(arg0);
	}

	@Override
	public Set<String> keySet() {
		return getMap().keySet();
	}

	@Override
	public Object put(String arg0, Object arg1) {
		return getMap().put(arg0, arg1);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void putAll(BSONObject arg0) {
		getMap().putAll(arg0.toMap());
		
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void putAll(Map arg0) {
		getMap().putAll(arg0);
	}

	@Override
	public Object removeField(String arg0) {
		return getMap().remove(arg0);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Map toMap() {
		return getMap();
	}
	
	private boolean partial = false;

	@Override
	public boolean isPartialObject() {
		return partial;
	}

	@Override
	public void markAsPartialObject() {
		partial = true;
	}

	
	public void setCreated(Date date) {
		getMap().put("created", date);
	}
	
	public Date getCreated() {
		return (Date) getMap().get("created");
	}
	
	public void setPrepared(boolean prepared) {
		getMap().put("prepared", prepared);
	}
	
	public boolean isPrepared() {
		return (Boolean) getMap().get("prepared");
	}
}
