package com.findwise.hydra;

import java.io.InputStream;
import java.util.List;

public interface PipelineReader<T extends DatabaseType> {
	
	InputStream getStream(DatabaseFile df);

	/**
	 * Returns a InputStream matching this filename, or null if none exist.
	 * 
	 * Should several files exist, which one is returned is up to the implementation.
	 */
	InputStream getStream(String fileName);
	
	List<DatabaseFile> getFiles();
	
	/**
	 * Returns a file with this filename, or null if none exist.
	 * 
	 * Should several files exist, which one is returned is up to the implementation.
	 */
	DatabaseFile getFile(String fileName);
	
	Pipeline<Stage> getPipeline();
	
	Pipeline<Stage> getDebugPipeline();
}