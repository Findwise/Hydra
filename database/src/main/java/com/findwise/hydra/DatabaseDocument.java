package com.findwise.hydra;

import java.util.Date;
import java.util.Set;

import com.findwise.hydra.Document;

public interface DatabaseDocument<T extends DatabaseType> extends Document<T> {

	Object putMetadataField(String key, Object value);

	boolean removeTouchedBy(String stage);
	
	boolean removeFetchedBy(String stage);
	
	boolean touchedBy(String stage);
	
	boolean fetchedBy(String stage);

	Set<String> getTouchedBy();
	
	Set<String> getFetchedBy();

	Date getTouchedTime(String stage);
	
	Date getFetchedTime(String stage);
	
	Date getCompletedTime();
	
	String getCompletedBy();

	void setID(DocumentID<T> id);
}
