package com.findwise.hydra;

public interface StatusReader<T extends DatabaseType> {
	PipelineStatus<T> getStatus();
	
	/**
	 * @return true if there is a status saved, otherwise false.
	 */
	boolean hasStatus();
}
