package com.findwise.hydra;

/**
 * This interface defines the settings that are supported
 * from the Admin UI, such as whether or not to keep old documents.
 * 
 * Consequently, the actual object implementing this should
 * be stored in a location where it is accessible from the Admin
 * UI and an instance of Hydra Core.
 * 
 * @author joel.westberg
 *
 */
public interface PipelineStatus {

	boolean DEFAULT_DISCARD_OLD = true;
	long DEFAULT_NUMBER_TO_KEEP = 1000;
	
	/**
	 * Set whether or not to discard old documents.
	 * 
	 * @param discard
	 */
	void setDiscardOldDocuments(boolean discardOld);
	
	boolean isDiscardingOldDocuments();
	
	/**
	 * If set to discard old documents, this dictates how many to keep.
	 * 
	 * @param number
	 */
	void setDiscardedToKeep(long numberToKeep);
	
	long getNumberToKeep();
	
}
