package com.findwise.hydra.memorydb;

import java.util.HashMap;
import java.util.Map;

import com.findwise.hydra.DatabaseQuery;
import com.findwise.hydra.local.LocalQuery;

public class MemoryQuery extends LocalQuery implements DatabaseQuery<MemoryType> {

	private Map<String, Object> metadataEqualsMap;
	private Map<String, Boolean> metadataExistsMap;
	private Map<String, Boolean> fetchedByMap;
	
	public MemoryQuery() {
		super();
		metadataEqualsMap = new HashMap<String, Object>();
		metadataExistsMap = new HashMap<String, Boolean>();
		fetchedByMap = new HashMap<String, Boolean>();
	}

	@Override
	public void requireMetadataFieldEquals(String fieldName, Object o) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void requireMetadataFieldNotEquals(String fieldName, Object o) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void requireMetadataFieldExists(String fieldName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void requireMetadataFieldNotExists(String fieldName) {
		// TODO Auto-generated method stub
		
	}
	
	public Map<String, Object> getMetadataEquals() {
		return metadataEqualsMap;
	}
	
	public Map<String, Boolean> getMetadataExists() {
		return metadataExistsMap;
	}
	
	public void requireFetchedByStage(String tag) {
		fetchedByMap.put(tag, true);
	}

	public void requireNotFetchedByStage(String tag) {
		fetchedByMap.put(tag, false);
	}

	public Map<String, Boolean> getFetchedBy() {
		return fetchedByMap;
	}
}
