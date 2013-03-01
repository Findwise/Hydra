package com.findwise.hydra;

import com.findwise.hydra.Query;

public interface DatabaseQuery<T extends DatabaseType> extends Query {
	
	void requireContentFieldNotEquals(String fieldName, Object o);
	
	void requireMetadataFieldEquals(String fieldName, Object o);
	
	void requireMetadataFieldNotEquals(String fieldName, Object o);
	
	void requireMetadataFieldExists(String fieldName);
	
	void requireMetadataFieldNotExists(String fieldName);

	void requireNotFetchedByStage(String tag);
}
