package com.findwise.hydra;

import java.io.IOException;
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

	List<DatabaseDocument<T>> getDocuments(DatabaseQuery<T> q, int limit);

	DocumentFile getDocumentFile(DatabaseDocument<T> d) throws IOException;
	
	/**
	 * @return the number of active (i.e. not processed or discarded) documents in database
	 */
	long getActiveDatabaseSize();
	
	/**
	 * @return the number of inactive (i.e. processed or discarded) documents in the database.
	 */
	long getInactiveDatabaseSize();
}
