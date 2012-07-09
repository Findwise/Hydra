package com.findwise.hydra.common;

import java.util.Map;
import java.util.Set;

public interface Document extends JsonDeserializer, JsonSerializer {
	
	public enum Action {
		ADD, DELETE, UPDATE
	};
	
	enum Status { PROCESSING, PROCESSED, DISCARDED, FAILED, PENDING };

	String ID_KEY = "_id";
	String ACTION_KEY = "_action";
	String CONTENTS_KEY = "contents";
	String METADATA_KEY = "metadata";
	String PENDING_METADATA_FLAG = "pending";
	String PROCESSED_METADATA_FLAG = "processed";
	String DISCARDED_METADATA_FLAG = "discarded";
	String FETCHED_METADATA_TAG = "fetched";
	String TOUCHED_METADATA_TAG = "touched";
	String FAILED_METADATA_FLAG = "failed";
	String DATE_METADATA_SUBKEY = "date";
	String STAGE_METADATA_SUBKEY = "stage";
	String ERROR_METADATA_KEY = "error";
	
	Action getAction();
	
	void setAction(Action action);
	
	Status getStatus();
	
	boolean hasContentField(String fieldName);

	boolean hasMetadataField(String fieldName);

	Object getID();

	Object putContentField(String fieldName, Object value);

	Object getContentField(String fieldName);

	Map<String, Object> getMetadataMap();

	void addError(String from, Throwable t);

	boolean hasErrors();

	Set<String> getContentFields();

	void putAll(Document d);

	boolean isEqual(Document d);

	void clear();

	/**
	 * Implementations are expected to handle data on the form on the form: {
	 * Document.ID_KEY : &lt;id&gt;, Document.CONTENTS_KEY : { ... },
	 * Document.METADATA_KEY : { ... } }
	 */
	@Override
	void fromJson(String json) throws JsonException;

	/**
	 * Implementations are expected to output data on the form on the form: {
	 * Document.ID_KEY : &lt;id&gt;, Document.CONTENTS_KEY : { ... },
	 * Document.METADATA_KEY : { ... } }
	 */
	@Override
	String toJson();

	/**
	 * Returns a JSON representation of the requested content fields, and the ID
	 * of this document.
	 * 
	 * Nullsafe.
	 */
	String contentFieldsToJson(Iterable<String> contentFields);

	/**
	 * Returns a JSON representation of the requested metadata fields, and the
	 * ID of this document.
	 * 
	 * Nullsafe.
	 */
	String metadataFieldsToJson(Iterable<String> contentFields);
}
