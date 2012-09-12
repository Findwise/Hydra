package com.findwise.hydra;

import java.util.HashMap;
import java.util.Map;

/**
 * Convenience class for map-based implementation of PipelineStatus
 * 
 *
 */
public abstract class AbstractPipelineStatus<T extends DatabaseType> implements PipelineStatus<T> {
	private Map<String, Object> map = new HashMap<String, Object>();

	private static final String DISCARDS_OLD_KEY = "discardOld";
	
	@Override
	public void setDiscardOldDocuments(boolean discardOld) {
		if(discardOld) {
			map.put(DISCARDS_OLD_KEY, getNumberToKeep());
		} else {
			map.remove(DISCARDS_OLD_KEY);
		}
	}
	
	protected Map<String, Object> getMap() {
		return map;
	}

	@Override
	public boolean isDiscardingOldDocuments() {
		return map.containsKey(DISCARDS_OLD_KEY);
	}

	@Override
	public void setDiscardedToKeep(long numberToKeep) {
		map.put(DISCARDS_OLD_KEY, numberToKeep);
	}

	@Override
	public long getNumberToKeep() {
		return (Long) map.get(DISCARDS_OLD_KEY);
	}

	@Override
	public void setDiscardedMaxSize(int maxSize) {
		map.put("discard_size", maxSize);
	}

	@Override
	public int getDiscardedMaxSize() {
		return (Integer) map.get("discard_size");
	}

	@Override
	public int getDiscardedCount() {
		return (Integer) map.get("discarded_count");
	}

	@Override
	public void setDiscardedCount(int i) {
		map.put("discarded_count", i);
		
	}

	@Override
	public int getProcessedCount() {
		return (Integer) map.get("processed_count");
	}

	@Override
	public void setProcessedCount(int i) {
		map.put("processed_count", i);
		
	}

	@Override
	public int getFailedCount() {
		return (Integer) map.get("failed_count");
	}

	@Override
	public void setFailedCount(int i) {
		map.put("failed_count", i);
	}
}
