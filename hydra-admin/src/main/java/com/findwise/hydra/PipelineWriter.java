package com.findwise.hydra;

import java.io.IOException;
import java.io.InputStream;

public interface PipelineWriter<T extends DatabaseType> {

	/**
	 * Writes a pipeline to the database, using the writeStage method for
	 * individual stages.
	 * 
	 */
	void write(Pipeline<? extends Stage> c) throws IOException;
	
	/**
	 * Saves a file to the database.
	 * 
	 * @return the id of the written file
	 */
	Object save(String filename, InputStream file);
	
	/**
	 * Removes a file from the database.
	 * @param id
	 * @return
	 */
	boolean deleteFile(Object id);

	/**
	 * Removes all inactive files from the configuration database.
	 */
	void removeInactiveFiles();

	/**
	 * This method will be run if the Connector determines that the Database is
	 * brand new, and needs to be set up in some way.
	 */
	void prepare();
}
