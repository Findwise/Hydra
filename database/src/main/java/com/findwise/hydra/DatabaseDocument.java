package com.findwise.hydra;

import com.findwise.hydra.common.Document;

public interface DatabaseDocument<T extends DatabaseType> extends Document {

	Object putMetadataField(String key, Object value);

	boolean removeTouchedBy(String stage);
	
	boolean removeFetchedBy(String stage);
	
	boolean touchedBy(String stage);
	
	boolean fetchedBy(String stage);
	
}
