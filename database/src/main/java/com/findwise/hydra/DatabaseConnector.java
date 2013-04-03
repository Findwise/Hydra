package com.findwise.hydra;

import java.io.IOException;

import com.findwise.hydra.Document;
import com.findwise.hydra.Query;

public interface DatabaseConnector<T extends DatabaseType> {
	
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
	
	PipelineReader getPipelineReader();
	
	PipelineWriter getPipelineWriter();
	
	DocumentReader<T> getDocumentReader();
	
	DocumentWriter<T> getDocumentWriter();
	
	DatabaseQuery<T> convert(Query query);
	
	DatabaseDocument<T> convert(Document<?> document) throws ConversionException;
	
	boolean isConnected();

	StatusWriter<T> getStatusWriter();
	
	StatusReader<T> getStatusReader();
	
	public class ConversionException extends Exception {
		/**
		 * Auto generated
		 */
		private static final long serialVersionUID = -5921377046346967643L;
		
		public ConversionException(String msg) {
			super(msg);
		}
		
		public ConversionException(String msg, Throwable t) {
			super(msg, t);
		}
		
	}
}
