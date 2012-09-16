package com.findwise.hydra;

import java.io.IOException;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.LocalQuery;

public interface DatabaseConnector<T extends DatabaseType> {
	String DATABASE_URL_PARAM = "database_url";
	String DATABASE_USER = "database_user";
	String DATABASE_PASSWORD = "database_password";
	String NAMESPACE_PARAM = "namespace";
	String OLD_MAX_COUNT = "old_max_count";
	String OLD_MAX_SIZE_MB = "old_max_size_mb";
	
	/**
	 * Connect to the database.
	 */
	void connect() throws IOException;

	/**
	 * Will block execution until the latest write has been pushed though.
	 * Should really only be used for testing, the system is designed to be
	 * asynchronous. 
	 */
	void waitForWrites(boolean alwaysBlocking);
	
	boolean isWaitingForWrites();
	
	PipelineReader<T> getPipelineReader();
	
	PipelineWriter<T> getPipelineWriter();
	
	DocumentReader<T> getDocumentReader();
	
	DocumentWriter<T> getDocumentWriter();
	
	DatabaseQuery<T> convert(LocalQuery query);

	DatabaseDocument<T> convert(LocalDocument document);

	boolean isConnected();

	StatusWriter<T> getStatusWriter();
	
	StatusReader<T> getStatusReader();
	
}
