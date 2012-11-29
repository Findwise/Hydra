package com.findwise.hydra.memorydb;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.local.LocalDocument;

public class MemoryDocument extends LocalDocument implements DatabaseDocument<MemoryType> {
	
	@Override
	public Object putMetadataField(String key, Object value) {
		return getMetadataMap().put(key, value);
	}
	
	public boolean matches(MemoryQuery mq) {
		for(Map.Entry<String, Boolean> touched  : mq.getTouched().entrySet()) {
			if(touched.getValue()) {
				if(!touchedBy(touched.getKey())) {
					return false;
				}
			}
			else {
				if(touchedBy(touched.getKey())) {
					return false;
				}
			}
		}
		
		if(mq.getAction() != null && mq.getAction() != getAction()) {
			return false;
		}
		
		for(Map.Entry<String, Object> e : mq.getContentsEquals().entrySet()) {
			if(!hasContentField(e.getKey()) || !getContentField(e.getKey()).equals(e.getValue())) {
				return false;
			}
		}
		
		for(Map.Entry<String, Boolean> e : mq.getMetadataExists().entrySet()) {
			if(e.getValue()) {
				if(!hasMetadataField(e.getKey())) {
					return false;
				}
			} else {
				if(hasMetadataField(e.getKey())) {
					return false;
				}
			}
		}
		
		for(Map.Entry<String, Object> e : mq.getMetadataEquals().entrySet()) {
			if(!hasMetadataField(e.getKey()) || !getMetadataMap().get(e.getKey()).equals(e.getValue())) {
				return false;
			}
		}
		
		for(Map.Entry<String, Boolean> e : mq.getContentsExists().entrySet()) {
			if(e.getValue()) {
				if(!hasContentField(e.getKey())) {
					return false;
				}
			} else {
				if(hasContentField(e.getKey())) {
					return false;
				}
			}
		}
		
		for(Map.Entry<String, Boolean> e : mq.getFetchedBy().entrySet()) {
			if(e.getValue()) {
				if(!fetchedBy(e.getKey())) {
					return false;
				}
			} else {
				if(fetchedBy(e.getKey())) {
					return false;
				}
			}
		}
		
		return true;
	}

	/**
	 * nullsafe
	 */
	@SuppressWarnings("unchecked")
	private Map<String, Object> getMetadataSubMap(String key) {
		if(getMetadataMap().containsKey(key)) {
			return (Map<String, Object>) getMetadataMap().get(key);
		}
		return new HashMap<String, Object>();
	}

	@Override
	public boolean touchedBy(String stage) {
		return getMetadataSubMap(TOUCHED_METADATA_TAG).containsKey(stage);
	}

	@Override
	public boolean fetchedBy(String stage) {
		return getMetadataSubMap(FETCHED_METADATA_TAG).containsKey(stage);
	}
	
	@SuppressWarnings("unchecked")
	protected void tag(String tag, String stage) {
		if(!getMetadataMap().containsKey(tag)) {
			getMetadataMap().put(tag, new HashMap<String, Object>());
		}
		((Map<String, Object>) getMetadataMap().get(tag)).put(stage, new Date());
	}

	public void setID(Object id) {
		getDocumentMap().put(ID_KEY, id);
	}
	
	@Override
	public Object getID() {
		return getDocumentMap().get(ID_KEY);
	}

	@Override
	public boolean removeFetchedBy(String stage) {
		return getMetadataSubMap(FETCHED_METADATA_TAG).remove(stage) != null;
	}
	
	@Override
	public boolean removeTouchedBy(String stage) {
		return getMetadataSubMap(TOUCHED_METADATA_TAG).remove(stage) != null;
	}
	
	@Override
	public Object removeContentField(String key) {
		return getContentMap().remove(key);
	}

	@Override
	public Set<String> getTouchedBy() {
		return getMetadataSubMap(TOUCHED_METADATA_TAG).keySet();
	}

	@Override
	public Set<String> getFetchedBy() {
		return getMetadataSubMap(FETCHED_METADATA_TAG).keySet();
	}

	@Override
	public Date getTouchedTime(String stage) {
		return (Date) getMetadataSubMap(TOUCHED_METADATA_TAG).get(stage);
	}

	@Override
	public Date getFetchedTime(String stage) {
		return (Date) getMetadataSubMap(FETCHED_METADATA_TAG).get(stage);
	}
	@Override
	public Date getCompletedTime() {
		return getDoneTag().date;
	}

	@Override
	public String getCompletedBy() {
		return getDoneTag().tag;
	}
	
	private class DoneTuple {
		private Date date;
		private String tag;
	}
	
	private DoneTuple getDoneTag() {
		DoneTuple done = new DoneTuple();
		
		if(getMetadataMap().containsKey(FAILED_METADATA_FLAG)) {
			done.date = (Date) getMetadataSubMap(FAILED_METADATA_FLAG).get(DATE_METADATA_SUBKEY);
			done.tag = (String) getMetadataSubMap(FAILED_METADATA_FLAG).get(STAGE_METADATA_SUBKEY);
		}
		else if(getMetadataMap().containsKey(DISCARDED_METADATA_FLAG)) {
			done.date = (Date) getMetadataSubMap(DISCARDED_METADATA_FLAG).get(DATE_METADATA_SUBKEY);
			done.tag = (String) getMetadataSubMap(DISCARDED_METADATA_FLAG).get(STAGE_METADATA_SUBKEY);
		}
		else if(getMetadataMap().containsKey(PENDING_METADATA_FLAG)) {
			done.date = (Date) getMetadataSubMap(DISCARDED_METADATA_FLAG).get(DATE_METADATA_SUBKEY);
			done.tag = (String) getMetadataSubMap(DISCARDED_METADATA_FLAG).get(STAGE_METADATA_SUBKEY);
		}
		else if(getMetadataMap().containsKey(PROCESSED_METADATA_FLAG)) {
			done.date = (Date) getMetadataSubMap(DISCARDED_METADATA_FLAG).get(DATE_METADATA_SUBKEY);
			done.tag = (String) getMetadataSubMap(DISCARDED_METADATA_FLAG).get(STAGE_METADATA_SUBKEY);
		}
		return done;
	}
}
