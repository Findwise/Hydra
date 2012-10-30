package com.findwise.hydra.mongodb;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.bson.BSONObject;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.common.Document;
import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.tools.Comparator;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


public class MongoDocument implements DBObject, DatabaseDocument<MongoType> {
	private DBObject documentMap;
	
	private static Logger logger = LoggerFactory.getLogger(MongoDocument.class);
	
	private boolean actionTouched = false;
	
	private Set<String> touchedContent;
	private Set<String> touchedMetadata;
	
	public static final String MONGO_ID_KEY = "_id";
	private boolean partialObject = false;
	
	public MongoDocument() {
		setup();
	}
	
	public MongoDocument(String json) throws JsonException {
		this();
		fromJson(json);
	}
	
	private void setup() {
		touchedContent = new HashSet<String>();
		touchedMetadata = new HashSet<String>();
		documentMap = new BasicDBObject();
		documentMap.put(METADATA_KEY, new BasicDBObject());
		documentMap.put(CONTENTS_KEY, new BasicDBObject());
	}

	
	private DBObject getContents() {
		return (DBObject) documentMap.get(CONTENTS_KEY);
	}
	
	private DBObject getMetadata() {
		return (DBObject) documentMap.get(METADATA_KEY);
	}
	
	@Override
	public Action getAction() {
		Object o = documentMap.get(ACTION_KEY);
		if(o!=null) {
			return Action.valueOf((String)o);
		}
		return null;
	}
	
	@Override
	public void setAction(Action action) {
		actionTouched = true;
		if(action==null) {
			documentMap.put(ACTION_KEY, null);
		} else {
			documentMap.put(ACTION_KEY, action.toString());
		}
	}

	@Override
	public final void putAll(BSONObject o) {
		documentMap.putAll(o.toMap());
	}

	@Override
	public Object get(String key) {
		return documentMap.get(key);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> toMap() {
		return documentMap.toMap();
	}

	@Override
	public Object removeField(String key) {
		return documentMap.removeField(key);
	}

	@Override
	public boolean hasErrors() {
		return getMetadata().containsField(ERROR_METADATA_KEY);
	}
	
	@Override
	public void addError(String from, Throwable t) {
		if(!hasErrors()) {
			getMetadata().put(ERROR_METADATA_KEY, new BasicDBObject());
		}
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		t.printStackTrace(pw);
		
		((BasicDBObject)getMetadata().get(ERROR_METADATA_KEY)).put(from, sw.toString());
		touchedMetadata.add(ERROR_METADATA_KEY);
	}
	
	@Override
	@Deprecated
	public boolean containsKey(String s) {
		return containsField(s);
	}

	@Deprecated
	@Override
	/**
	 * This method will in most cases only return true for keys "_id", "metadata" and "content".
	 * Use containsMetadataField or containsContentField instead to look inside those maps.
	 */
	public boolean containsField(String key) {
		return documentMap.containsKey(key);
	}
	
	@Override
	public boolean hasMetadataField(String key) {
		return getMetadataField(key)!=null;
	}
	
	@Override
	public boolean hasContentField(String key) {
		return getContentField(key)!=null;
	}

	@Override
	public void markAsPartialObject() {
		partialObject = true;
	}

	@Override
	public boolean isPartialObject() {
		return partialObject;
	}

	@Override
	@Deprecated
	public Object put(String key, Object v) {
		return documentMap.put(key, v);
	}
	
	@Override
	public final Object putContentField(String key, Object v) {
		touchedContent.add(key);
		return getContents().put(key, v);
	}
	
	public Object getContentField(String key) {
		return getContents().get(key);
	}
	
	public Object getMetadataField(String key) {
		return getMetadata().get(key);
	}
	
	@SuppressWarnings("unchecked")
	public Map<String, Object> getMetadataMap() {
		return getMetadata().toMap();
	}

	
	@SuppressWarnings("unchecked")
	public Map<String, Object> getContentsMap() {
		return getContents().toMap();
	}
	
	@Override
	public final Object putMetadataField(String key, Object v) {
		touchedMetadata.add(key);
		return getMetadata().put(key, v);
	}
	
	@Override
	public Object getID() {
		return get(MONGO_ID_KEY);
	}

	public String getIDKey() {
		return MONGO_ID_KEY;
	}

	@SuppressWarnings({ "rawtypes" })
	@Override
	public final void putAll(Map m) {
		documentMap.putAll(m);
	}
	
	protected static ObjectId toObjectId(Map<String, Object> m) {
		if(m.containsKey("_time") && m.containsKey("_machine") && m.containsKey("_inc")) {
			return new ObjectId((Integer)m.get("_time"), (Integer)m.get("_machine"), (Integer)m.get("_inc"));
		}
		return null;
	}

	@SuppressWarnings({ "unchecked" })
	@Override
	public final void putAll(Document d) {
		if(d==null) {
			logger.warn("null passed to MongoDocument.putAll(), doing nothing.");
			return;
		}
		
		if(d.getID() instanceof Map) {
			documentMap.put(MONGO_ID_KEY, toObjectId((Map<String, Object>) d.getID()));
		}
		else {
			documentMap.put(MONGO_ID_KEY, d.getID());
		}
		
		if(d.getAction()!=null) {
			setAction(d.getAction());
		}
		
		for(Map.Entry<String, Object> e : d.getMetadataMap().entrySet()) {
			putMetadataField(e.getKey(), e.getValue());
		}
		
		for(String s : d.getContentFields()) {
			putContentField(s, d.getContentField(s));
		}
	}

	@Override
	public Set<String> keySet() {
		return documentMap.keySet();
	}
	
	public String toString() {
		return documentMap.toString();
	}
	
	public boolean isActionTouched() {
		return actionTouched;
	}

	public Set<String> getTouchedContent() {
		return touchedContent;
	}

	public Set<String> getTouchedMetadata() {
		return touchedMetadata;
	}

	private Set<String> getMetadataFields() {
		return getMetadata().keySet();
	}

	@Override
	public Set<String> getContentFields() {
		return getContents().keySet();
	}

	@Override
	public boolean isEqual(Document d) {
		if(d.getID()!=null) {
			if(!d.getID().equals(getID())) {
				return false;
			}
		}
		else {
			if(getID()!=null) {
				return false;
			}
		}
		
		if(d.getAction()!=getAction()) {
			return false;
		}

		if(equalMetadata(d) && equalContent(d)) {
			return true;
		}
		
		return false;
	}
	
	private boolean equalMetadata(Document d) {
		Set<String> metadata = d.getMetadataMap().keySet();
		if(metadata.size()!=getMetadataFields().size()) {
			return false;
		}
		
		for(String s : metadata) {
			if(!getMetadataFields().contains(s)) {
				return false;
			}
			
			if(!Comparator.equals(getMetadataField(s), d.getMetadataMap().get(s))) {
				return false;
			}
		}

		return true;
	}
	
	private boolean equalContent(Document d) {
		Set<String> content = d.getContentFields();
		if(content.size()!=getContentFields().size()) {
			return false;
		}
		
		for(String s : content) {
			if(!getContentFields().contains(s)) {
				return false;
			}
			
			if(!Comparator.equals(getContentField(s), d.getContentField(s))) {
				return false;
			}
		}

		return true;
	}
	
	@Override
	public final void fromJson(String json) throws JsonException {
		clear();
		putAll(new LocalDocument(json));
	}
	
	@Override
	public String toJson() {
		LocalDocument ld = new LocalDocument();
		ld.putAll(this);
		return ld.toJson();
	}
	
	@Override
	public String contentFieldsToJson(Iterable<String> contentFields) {
		LocalDocument ld = new LocalDocument();
		ld.putAll(this);
		return ld.contentFieldsToJson(contentFields);
	}
	
	@Override
	public String metadataFieldsToJson(Iterable<String> metadataFields) {
		LocalDocument ld = new LocalDocument();
		ld.putAll(this);
		return ld.metadataFieldsToJson(metadataFields);
	}
	
	@Override
	public final void clear() {
		setup();
	}

	@Override
	public Status getStatus() {
		if(getMetadataMap().containsKey(FAILED_METADATA_FLAG)) {
			return Status.FAILED;
		}
		if(getMetadataMap().containsKey(DISCARDED_METADATA_FLAG)) {
			return Status.DISCARDED;
		}
		if(getMetadataMap().containsKey(PENDING_METADATA_FLAG)) {
			return Status.PENDING;
		}
		if(getMetadataMap().containsKey(PROCESSED_METADATA_FLAG)) {
			return Status.PROCESSED;
		}
		return Status.PROCESSING;
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
	@Override
	public boolean removeFetchedBy(String stage) {
		touchedMetadata.add(FETCHED_METADATA_TAG);
		return ((Map<String,Object>)getMetadataMap().get(FETCHED_METADATA_TAG)).remove(stage)!=null;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public boolean removeTouchedBy(String stage) {
		touchedMetadata.add(TOUCHED_METADATA_TAG);
		return ((Map<String,Object>)getMetadataMap().get(TOUCHED_METADATA_TAG)).remove(stage)!=null;
	}
}
