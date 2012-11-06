package com.findwise.hydra;

import java.util.List;

import com.findwise.hydra.common.DocumentFile;

/**
 * The implementation of this interface should be side-effect free, allowing the
 * fetching of documents and files with no functional side-effects in Hydra.
 * These methods should under no circumstance write to the central repository,
 * only read.
 * 
 * @author joel.westberg
 */
public interface DocumentReader<T extends DatabaseType> {

	DatabaseDocument<T> getDocument(DatabaseQuery<T> q);

	DatabaseDocument<T> getDocumentById(Object id);

	DatabaseDocument<T> getDocumentById(Object id, boolean includeInactive);
	
	/**
	 * Returns a TailableIterator over all inactive documents, no longer being
	 * processed. 
	 */
	TailableIterator<T> getInactiveIterator();

	TailableIterator<T> getInactiveIterator(DatabaseQuery<T> query);
	
	List<DatabaseDocument<T>> getDocuments(DatabaseQuery<T> q, int limit);
	List<DatabaseDocument<T>> getDocuments(DatabaseQuery<T> q, int limit, int skip);
	long getNumberOfDocuments(DatabaseQuery<T> q);

	DocumentFile getDocumentFile(DatabaseDocument<T> d, String fileName);
	
	List<String> getDocumentFileNames(DatabaseDocument<T> d);
	
	/**
	 * @return the number of active (i.e. not processed or discarded) documents in database
	 */
	long getActiveDatabaseSize();
	
	/**
	 * @return the number of inactive (i.e. processed or discarded) documents in the database.
	 */
	long getInactiveDatabaseSize();
	
	Object toDocumentId(Object jsonPrimitive);
	
	Object toDocumentIdFromJson(String json);
}
