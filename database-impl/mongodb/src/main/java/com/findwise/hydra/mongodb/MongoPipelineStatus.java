package com.findwise.hydra.mongodb;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.bson.BSONObject;

import com.findwise.hydra.PipelineStatus;
import com.mongodb.DBObject;

public class MongoPipelineStatus implements PipelineStatus, DBObject {
	private Map<String, Object> map;

	private long numberToKeep = DEFAULT_NUMBER_TO_KEEP;
	
	public static final String DISCARDS_OLD_KEY = "discardOld";
	
	/**
	 * created defaults to NOW
	 * prepared defaults to false
	 */
	public MongoPipelineStatus() {
		map = new HashMap<String, Object>();
		setCreated(new Date());
		setPrepared(false);
	}
	
	@Override
	public boolean containsField(String arg0) {
		return map.containsKey(arg0);
	}

	@Override
	public boolean containsKey(String arg0) {
		return containsField(arg0);
	}

	@Override
	public Object get(String arg0) {
		return map.get(arg0);
	}

	@Override
	public Set<String> keySet() {
		return map.keySet();
	}

	@Override
	public Object put(String arg0, Object arg1) {
		return map.put(arg0, arg1);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void putAll(BSONObject arg0) {
		map.putAll(arg0.toMap());
		
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void putAll(Map arg0) {
		map.putAll(arg0);
	}

	@Override
	public Object removeField(String arg0) {
		return map.remove(arg0);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Map toMap() {
		return map;
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

	@Override
	public void setDiscardOldDocuments(boolean discardOld) {
		if(discardOld) {
			map.put(DISCARDS_OLD_KEY, getNumberToKeep());
		} else {
			map.remove(DISCARDS_OLD_KEY);
		}
	}

	@Override
	public boolean isDiscardingOldDocuments() {
		return map.containsKey(DISCARDS_OLD_KEY);
	}

	@Override
	public void setDiscardedToKeep(long numberToKeep) {
		this.numberToKeep = numberToKeep;
		
		if(isDiscardingOldDocuments()) {
			map.put(DISCARDS_OLD_KEY, numberToKeep);
		}
	}

	@Override
	public long getNumberToKeep() {
		return numberToKeep;
	}
	
	public void setCreated(Date date) {
		map.put("created", date);
	}
	
	public Date getCreated() {
		return (Date) map.get("created");
	}
	
	public void setPrepared(boolean prepared) {
		map.put("prepared", prepared);
	}
	
	public boolean isPrepared() {
		return (Boolean) map.get("prepared");
	}
}
