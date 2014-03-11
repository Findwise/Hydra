package com.findwise.hydra;

import java.util.Collection;

/**
 * Interface specifying the operations that must be supported for a
 * DatabaseDocument cache.
 * 
 * Bear in mind when using, that a cache that does absolutely nothing {@link
 * NoopCache} is a valid implementation. As such it can not be trusted that just
 * because you added something to the cache, it means you can get it out again.
 * 
 * However, the cache does guarantee that a call to any getDocument-methods can
 * consistently reproduce the same document over and over, unless a
 * remove-method is invoked.
 * 
 * @author joel.westberg@findwise.com
 */
public interface Cache<T extends DatabaseType> {

	void prepare();

	void add(DatabaseDocument<T> doc);

	void add(Collection<DatabaseDocument<T>> docs);

	/**
	 * Removes a document with the specified id. 
	 * 
	 * @param id - the document to remove
	 * @return the removed document or null, if the document is not found
	 */
	DatabaseDocument<T> remove(DocumentID<T> id);

	/**
	 * Removes all documents from the cache. An implementation is not allowed to 
	 * return null for this method, and should instead return an empty collection.
	 * 
	 * @return the documents that have been removed. 
	 */
	Collection<DatabaseDocument<T>> removeAll();

	/**
	 * Removes and returns documents that have not been touched in the cache for
	 * longer than the supplied number of milliseconds.
	 * 
	 * An implementation is not allowed to 
	 * return null for this method, and should instead return an empty collection if necessary.
	 */
	Collection<DatabaseDocument<T>> removeStale(int stalerThanMs);
	
	/**
	 * Freshens a document on demand, keeping it from going stale.
	 */
	boolean freshen(DocumentID<T> id);

	DatabaseDocument<T> getDocumentById(DocumentID<T> id);

	DatabaseDocument<T> getDocument();

	DatabaseDocument<T> getDocument(DatabaseQuery<T> query);

	Collection<DatabaseDocument<T>> getDocument(DatabaseQuery<T> query,
			int limit);

	DatabaseDocument<T> getAndTag(DatabaseQuery<T> query, String ... tag);

	Collection<DatabaseDocument<T>> getAndTag(DatabaseQuery<T> query, int n,
			String ... tag);

	boolean markTouched(DocumentID<T> id, String tag);

	boolean update(DatabaseDocument<T> document);

	int getSize();
}
