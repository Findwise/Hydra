package com.findwise.hydra.memorydb;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.Document;
import com.findwise.hydra.DocumentID;
import com.findwise.hydra.JsonException;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.LocalDocumentID;

public class MemoryDocument implements DatabaseDocument<MemoryType> {
	
	private LocalDocument doc;
	
	public MemoryDocument() {
		doc = new LocalDocument();
	}
	
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

	@Override
	public void setID(DocumentID<MemoryType> id) {
		doc.setID(new LocalDocumentID(id.getID()));
	}
	
	@Override
	public MemoryDocumentID getID() {
		return new MemoryDocumentID(doc.getID());
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

	@Override
	public Action getAction() {
		return doc.getAction();
	}

	@Override
	public void setAction(Action action) {
		doc.setAction(action);
	}

	@Override
	public Status getStatus() {
		return doc.getStatus();
	}

	@Override
	public boolean hasContentField(String fieldName) {
		return doc.hasContentField(fieldName);
	}

	@Override
	public boolean hasMetadataField(String fieldName) {
		return doc.hasMetadataField(fieldName);
	}

	@Override
	public Object putContentField(String fieldName, Object value) {
		return doc.putContentField(fieldName, value);
	}

	@Override
	public Object getContentField(String fieldName) {
		return doc.getContentField(fieldName);
	}

	@Override
	public Map<String, Object> getMetadataMap() {
		return doc.getMetadataMap();
	}

	@Override
	public Map<String, Object> getContentMap() {
		return doc.getContentMap();
	}

	@Override
	public void addError(String from, Throwable t) {
		doc.addError(from, t);
	}

	@Override
	public boolean hasErrors() {
		return doc.hasErrors();
	}

	@Override
	public Set<String> getContentFields() {
		return doc.getContentFields();
	}

	@Override
	public void putAll(Document<?> d) {
		doc.putAll(d);
	}

	@Override
	public boolean isEqual(Document<?> d) {
		return doc.isEqual(d);
	}

	@Override
	public void clear() {
		doc.clear();
	}

	@Override
	public void fromJson(String json) throws JsonException {
		doc.fromJson(json);
	}

	@Override
	public String toJson() {
		return doc.toJson();
	}

	@Override
	public String contentFieldsToJson(Iterable<String> contentFields) {
		return doc.contentFieldsToJson(contentFields);
	}

	@Override
	public String metadataFieldsToJson(Iterable<String> metadataFields) {
		return doc.metadataFieldsToJson(metadataFields);
	}

	public void markSynced() {
		doc.markSynced();
	}

	public Set<String> getTouchedContent() {
		return doc.getTouchedContent();
	}

	public Collection<? extends String> getTouchedMetadata() {
		return doc.getTouchedMetadata();
	}

	public boolean isTouchedAction() {
		return doc.isTouchedAction();
	}
}
