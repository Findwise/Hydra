package com.findwise.hydra;

import java.util.List;

import com.findwise.hydra.DocumentFile;

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

	DatabaseDocument<T> getDocumentById(DocumentID<T> id);

	DatabaseDocument<T> getDocumentById(DocumentID<T> id, boolean includeInactive);
	
	/**
	 * Returns a TailableIterator over all inactive documents, no longer being
	 * processed. 
	 */
	TailableIterator<T> getInactiveIterator();

	TailableIterator<T> getInactiveIterator(DatabaseQuery<T> query);
	
	List<DatabaseDocument<T>> getDocuments(DatabaseQuery<T> q, int limit);
	List<DatabaseDocument<T>> getDocuments(DatabaseQuery<T> q, int limit, int skip);
	long getNumberOfDocuments(DatabaseQuery<T> q);

	DocumentFile<T> getDocumentFile(DatabaseDocument<T> d, String fileName);
	
	List<String> getDocumentFileNames(DatabaseDocument<T> d);
	
	/**
	 * @return the number of active (i.e. not processed or discarded) documents in database
	 */
	long getActiveDatabaseSize();
	
	/**
	 * @return the number of inactive (i.e. processed or discarded) documents in the database.
	 */
	long getInactiveDatabaseSize();
	
	DocumentID<T> toDocumentId(Object jsonPrimitive);
	
	DocumentID<T> toDocumentIdFromJson(String json);
}
