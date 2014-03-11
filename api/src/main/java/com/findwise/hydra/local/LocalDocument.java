package com.findwise.hydra.local;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.JsonParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.Document;
import com.findwise.hydra.DocumentFile;
import com.findwise.hydra.DocumentFileRepository;
import com.findwise.hydra.DocumentID;
import com.findwise.hydra.JsonException;
import com.findwise.hydra.SerializationUtils;
import com.findwise.tools.Comparator;

import static com.google.common.base.Preconditions.checkNotNull;

public class LocalDocument implements Document<Local> {
    private static Logger internalLogger = LoggerFactory.getLogger("internal");

	private Map<String, Object> documentMap;
	private Set<String> touchedContent;

	private Set<String> touchedMetadata;
	private boolean touchedAction;

	// The documentFileRepository is set by a setter instead of in the constructor, since
	// the class is also used in the core for serialization / deserialization. If it is unset,
	// getFile et.al. will throw NPE. // TODO: Still code smell...
	private DocumentFileRepository documentFileRepository;

	private boolean discardAfterProcessing = false;

	public LocalDocument() {
		documentMap = new HashMap<String, Object>();
		documentMap.put(CONTENTS_KEY, new HashMap<String, Object>());
		documentMap.put(METADATA_KEY, new HashMap<String, Object>());
		touchedContent = new HashSet<String>();
		touchedMetadata = new HashSet<String>();
		touchedAction = false;
	}
	
	public LocalDocument(String json) throws JsonException {
		this();
		fromJson(json);

		// The touched-sets will have been updated by fromJson, 
		// let's clean that up in this case
		markSynced();
	}

	public LocalDocument(LocalDocument doc) {
		this();
		try {
			fromJson(doc.toJson());
		} catch (JsonException e) {
			// I don't want to expose that we are reusing the json stuff for copying
			// Also, this should *never* happen since doc.toJson() should hopefully
			// always return valid json.
			throw new RuntimeException(e);
		}
		touchedContent = new HashSet<String>(doc.touchedContent);
		touchedMetadata = new HashSet<String>(doc.touchedMetadata);
		// The following objects are immutable so don't need to be copied
		touchedAction = doc.touchedAction;
		documentFileRepository = doc.documentFileRepository;
	}

	@Override
	public Action getAction() {
		return (Action) documentMap.get(ACTION_KEY);
	}
	
	@Override
	public void setAction(Action action) {
		documentMap.put(ACTION_KEY, action);
		touchedAction = true;
	}
	
	/**
	 * Marks all outstanding changes as in sync with the database.
	 */
	public final void markSynced() {
		touchedContent.clear();
		touchedMetadata.clear();
		touchedAction = false;
	}
	
	/**
	 * @return true if there are no outstanding changes
	 */
	public boolean isSynced() {
		return (touchedContent.size() + touchedMetadata.size()) == 0 && !touchedAction;
	}

	@Override
	public boolean hasContentField(String fieldName) {
		return getContentMap().containsKey(fieldName) && getContentMap().get(fieldName) != null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean hasMetadataField(String fieldName) {
		return ((Map<String, Object>)documentMap.get(METADATA_KEY)).containsKey(fieldName);
	}

	@Override
	public LocalDocumentID getID() {
		if(!documentMap.containsKey(ID_KEY)) {
			return null;
		}
		return new LocalDocumentID(documentMap.get(ID_KEY));
	}
	
	public void setID(DocumentID<Local> id) {
		documentMap.put(ID_KEY, id.getID());
	}
	
	/**
	 * Returns the backing map of this document. Beware that any changes 
	 * to this structure directly, will not be saved properly! If you wish
	 * to modify, use removeContentField() and putContentField() instead.
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Object> getContentMap() {
		return ((Map<String, Object>)documentMap.get(CONTENTS_KEY));
	}
	
	@SuppressWarnings("unchecked")
	public Map<String, Object> getMetadataMap() {
		return ((Map<String, Object>)documentMap.get(METADATA_KEY));
	}

	@Override
	public final Object putContentField(String fieldName, Object value) {
		fieldName = removePeriodFromKey(fieldName);
		touchedContent.add(fieldName);
		return getContentMap().put(fieldName, value);
	}

	private Object putMetadataField(String fieldName, Object value) {
		fieldName = removePeriodFromKey(fieldName);
		touchedMetadata.add(fieldName);
		return getMetadataMap().put(fieldName, value);
	}

	/**
	 * Appends a value to a content field, converting the field into a list if it is not already one.
	 * <strong>WARNING: This method does not check the type of inserted values, or the type of the field it appends to</strong>
	 *
	 * @param fieldName content field
	 * @param value the value to append with
	 */
	@SuppressWarnings("unchecked")
	public void appendToContentField(String fieldName, Object value) {
		List<Object> list = null;
		if (hasContentField(fieldName)) {
			Object fieldValue = getContentField(fieldName);
			if (fieldValue instanceof List<?>) {
				list = (List<Object>)fieldValue;
				list.add(value);
			} else {
				list = new ArrayList<Object>();
				list.add(fieldValue);
				list.add(value);
			}
		} else {
			list = new ArrayList<Object>();
			list.add(value);
		}
		putContentField(fieldName, list);
	}

	/**
	 * Get the value of a content field
	 *
	 * Beware that changes to this object will not be saved!
	 * Use putContentField() to update the value of a field.
	 *
	 * @param fieldName content field
	 * @return the value of the field
	 */
	@Override
	public Object getContentField(String fieldName) {
		return getContentMap().get(fieldName);
	}

	/**
	 * Gets and copies the value of a content field as a String
	 *
	 * @param fieldName content field
	 * @return the value in fieldName as a String
	 * @throws IncorrectFieldTypeException if the field does not contain a String
	 */
	public String getContentFieldAsString(String fieldName) throws IncorrectFieldTypeException {
		return getContentFieldAsType(fieldName, String.class);
	}

	/**
	 * Gets the value of a content field as a list of strings
	 *
	 * @param fieldName content field
	 * @return the value in fieldName as a list of strings, or an empty list if the field is empty
	 * @throws IncorrectFieldTypeException if the field does not contain a list
	 */
	@SuppressWarnings("unchecked")
	public List<String> getContentFieldAsStrings(String fieldName) throws IncorrectFieldTypeException {
		List<String> list;
		list = getContentFieldAsType(fieldName, List.class);
		if (null == list) {
			list = new ArrayList<String>();
		}
		return list;
	}

	/**
	 * Gets the value of a content field as a map
	 *
	 * @param fieldName content field
	 * @return the map in the content field, or an empty map if the field is empty
	 * @throws IncorrectFieldTypeException if the field does not contain a map
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Object> getContentFieldAsMap(String fieldName) throws IncorrectFieldTypeException {
		Map<String, Object> map;
		map = getContentFieldAsType(fieldName, Map.class);
		if (null == map) {
			map = new HashMap<String, Object>();
		}
		return map;
	}

	/**
	 * Gets the value of a content field as a long
	 *
	 * @param fieldName content field
	 * @return the value as a long
	 * @throws FieldIsEmptyException if the field is empty
	 * @throws IncorrectFieldTypeException if the field is not of type long
	 */
	public long getContentFieldAsLong(String fieldName) throws FieldIsEmptyException, IncorrectFieldTypeException {
		Number val;
		val = getContentFieldAsType(fieldName, Number.class);
		if (null != val) {
			return val.longValue();
		} else {
			throw new FieldIsEmptyException("Field '" + fieldName + "' is empty");
		}
	}

	/**
	 * Gets the value of a content field as a long
	 *
	 * @param fieldName content field
	 * @param defaultValue the value to return if the field is not a long
	 * @return the value as a long, or defaultValue if the field is empty
	 */
	public long getContentFieldAsLong(String fieldName, long defaultValue) throws IncorrectFieldTypeException {
		Number val = getContentFieldAsType(fieldName, Number.class);
		if (null != val) {
			return val.longValue();
		} else {
			return defaultValue;
		}
	}

	/**
	 * Gets the value of a content field as a double
	 *
	 * @param fieldName content field
	 * @return the value as a double
	 * @throws FieldIsEmptyException if the field is empty
	 * @throws IncorrectFieldTypeException if the field is not of type double
	 */
	public double getContentFieldAsDouble(String fieldName) throws FieldIsEmptyException, IncorrectFieldTypeException {
		Number val;
		val = getContentFieldAsType(fieldName, Number.class);
		if (null != val) {
			return val.doubleValue();
		} else {
			throw new FieldIsEmptyException("Field '" + fieldName + "' is empty");
		}
	}

	/**
	 * Gets the value of a content field as a double
	 *
	 * @param fieldName content field
	 * @param defaultValue the value to return if the field is not a double
	 * @return the value as a double, or defaultValue if the field is empty
	 */
	public double getContentFieldAsDouble(String fieldName, double defaultValue) throws IncorrectFieldTypeException {
		Number val = getContentFieldAsType(fieldName, Number.class);
		if (null != val) {
			return val.doubleValue();
		} else {
			return defaultValue;
		}
	}

	/**
	 * Gets the value of a content field as type T
	 *
	 * @param fieldName content field
	 * @param <T> the expected type
	 * @return the value of type T, or null if the content field is empty
	 * @throws IncorrectFieldTypeException if the field value cannot be cast to T
	 */
	public <T> T getContentFieldAsType(String fieldName, Class<? extends T> type) throws IncorrectFieldTypeException {
		if (hasContentField(fieldName)) {
			try {
				return type.cast(getContentField(fieldName));
			} catch (ClassCastException e) {
				throw new IncorrectFieldTypeException("Field '" + fieldName + "' is not of type '" + type.getCanonicalName() + "'");
			}
		}
		return null;
	}

	private Object getMetadataField(String fieldName) {
		return getMetadataMap().get(fieldName);
	}

	private Set<String> getMetadataFields() {
		return getMetadataMap().keySet();
	}

	@Override
	public Set<String> getContentFields() {
		HashSet<String> set = new HashSet<String>(getContentMap().keySet());
		Iterator<String> it = set.iterator();
		while(it.hasNext()) {
			if(!hasContentField(it.next())) {
				it.remove();
			}
		}
		return set;
	}
	
	@Override
	public boolean hasErrors() {
		return getMetadataMap().containsKey(ERROR_METADATA_KEY);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void addError(String from, Throwable t) {
		if(!hasErrors()) {
			getMetadataMap().put(ERROR_METADATA_KEY, new HashMap<String, String>());
		}
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		t.printStackTrace(pw);
		
		((HashMap)getMetadataMap().get(ERROR_METADATA_KEY)).put(from, sw.toString());
		touchedMetadata.add(ERROR_METADATA_KEY);
	}

	@SuppressWarnings("unchecked")
	@Override
	public final void fromJson(String json) throws JsonException {
		try {
			Map<String, Object> m = SerializationUtils.fromJson(json);
			if(m.containsKey(ID_KEY)) {
				documentMap.put(ID_KEY, m.get(ID_KEY));
			}
			if(m.containsKey(ACTION_KEY) && m.get(ACTION_KEY)!=null) {
				documentMap.put(ACTION_KEY, Action.valueOf((String)m.get(ACTION_KEY)));
			}
			if(m.containsKey(METADATA_KEY)) {
				Map<String, Object> metadata = (Map<String, Object>) m.get(METADATA_KEY);
				for(Map.Entry<String, Object> e : metadata.entrySet()) {
					putMetadataField(e.getKey(), metadata.get(e.getKey()));
				}
			}
			if(m.containsKey(CONTENTS_KEY)) {
				Map<String, Object> content = (Map<String, Object>) m.get(CONTENTS_KEY);
				for(Map.Entry<String, Object> e : content.entrySet()) {
					putContentField(e.getKey(), content.get(e.getKey()));
				}
			}
		} 
		catch(JsonParseException e) {
			internalLogger.error("Caught JsonParseException, throwing JsonException");
			throw new JsonException(e);
		}
	}
	
	private String removePeriodFromKey(String key) {
		if(key.contains(".")) {
			internalLogger.warn("The fieldname " + key + " contains a period, mongodb does not allow keys to contain a period (.). It has been replaced with a dash (-)");
			return key.replace(".", "-");
		}
		return key;
	}
	
	@Override
	public void putAll(Document<?> d) {
		if(d.getID() != null) {
			documentMap.put(ID_KEY, d.getID().getID());
		}
		
		if(d.getAction()!=null) {
			documentMap.put(ACTION_KEY, d.getAction());
		}
		for(Map.Entry<String, Object> e : d.getMetadataMap().entrySet()) {
			putMetadataField(e.getKey(), e.getValue());
		}
		for(String s : d.getContentFields()) {
			putContentField(s, d.getContentField(s));
		}
	}

	@Override
	public String toJson() {
		return SerializationUtils.toJson(documentMap);
	}
	
	protected Map<String, Object> getDocumentMap() {
		return documentMap;
	}

	@Override
	public boolean isEqual(Document<?> d) {
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
	
	private boolean equalMetadata(Document<?> d) {
		Set<String> metadata = getMetadataMap().keySet();
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
	
	private boolean equalContent(Document<?> d) {
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
	public String contentFieldsToJson(Iterable<String> contentFields) {
		return fieldsToJson(contentFields, null);
	}
	
	@Override
	public String metadataFieldsToJson(Iterable<String> metadataFields) {
		return fieldsToJson(null, metadataFields);
	}
	
	public String modifiedFieldsToJson() {
		return fieldsToJson(touchedContent, touchedMetadata);
	}
	
	/**
	 * Must be nullsafe, for all parameters and other operations. 
	 * @param contentFields
	 * @param metadataFields
	 * @return json map with id, contents, metadata and action
	 */
	private String fieldsToJson(Iterable<String> contentFields, Iterable<String> metadataFields) {
		HashMap<String, Object> map = new HashMap<String, Object>();
		if(getID() != null) {
			map.put(ID_KEY, getID().getID());
		} else {
			map.put(ID_KEY, null);
		}
		if(contentFields!=null) {
			HashMap<String, Object> cmap = new HashMap<String, Object>();
			for(String s : contentFields) {
				cmap.put(s, getContentField(s));
			}
			map.put(CONTENTS_KEY, cmap);
		}
		if(metadataFields!=null) {
			HashMap<String, Object> mmap = new HashMap<String, Object>();
			for(String s : metadataFields) {
				mmap.put(s, getMetadataField(s));
			}
			map.put(METADATA_KEY, mmap);
		}
		if(touchedAction) {
			map.put(ACTION_KEY, getAction());
		}
		return SerializationUtils.toJson(map);
	}
	
	public Map<String, Object> toMap() {
		return documentMap;
	}

	@Override
	public void clear() {
		documentMap.clear();
	}

	@Override
	public String toString() {
		return toJson();
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

	public Set<String> getTouchedContent() {
		return touchedContent;
	}

	public Set<String> getTouchedMetadata() {
		return touchedMetadata;
	}

	public boolean isTouchedAction() {
		return touchedAction;
	}

	@Override
	public Object removeContentField(String key) {
		return putContentField(key, null);
	}

	public void setDocumentFileRepository(DocumentFileRepository documentFileRepository) {
		this.documentFileRepository = documentFileRepository;
	}

	public List<String> getFileNames() {
		checkNotNull(documentFileRepository, "documentFileRepository is not set!");
		return documentFileRepository.getFileNames(getID());
	}

	public DocumentFile<Local> getFile(String fileName) {
		checkNotNull(documentFileRepository, "documentFileRepository is not set!");
		return documentFileRepository.getFile(fileName, getID());
	}

	public List<DocumentFile<Local>> getFiles() {
		checkNotNull(documentFileRepository, "documentFileRepository is not set!");
		return documentFileRepository.getFiles(getID());
	}

	public boolean saveFile(DocumentFile<Local> file) {
		checkNotNull(documentFileRepository, "documentFileRepository is not set!");
		return documentFileRepository.saveFile(file);
	}

	public boolean deleteFile(String fileName) {
		checkNotNull(documentFileRepository, "documentFileRepository is not set!");
		return documentFileRepository.deleteFile(fileName, getID());
	}

	/**
	 * Sets this document to be discarded after processing.
	 *
	 * N.B. Throwing an exception in a stage after discarding the document
	 * will still set the document as failed instead of discarded.
	 */
	public void discard() {
		this.discardAfterProcessing = true;
	}

	public boolean isDiscarded() {
		return discardAfterProcessing;
	}

}
