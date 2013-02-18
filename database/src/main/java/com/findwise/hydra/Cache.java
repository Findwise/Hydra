package com.findwise.hydra;

import java.util.Collection;



public interface Cache<T extends DatabaseType> {
	
	void prepare();
	
	void add(DatabaseDocument<T> doc);
	
	void add(Collection<DatabaseDocument<T>> docs);
	
	void remove(DocumentID id);
	
	void removeAll();
	
	DatabaseDocument<T> getDocumentById(DocumentID id);

	DatabaseDocument<T> getDocument();
	
	DatabaseDocument<T> getDocument(DatabaseQuery<T> query);
	
	Collection<DatabaseDocument<T>> getDocument(DatabaseQuery<T> query, int limit);
	
	DatabaseDocument<T> getAndTag(DatabaseQuery<T> query, String tag);

	Collection<DatabaseDocument<T>> getAndTag(DatabaseQuery<T> query, String tag, int n);

	boolean markTouched(DocumentID id, String tag);

	boolean markProcessed(DocumentID id, String stage);
	
	boolean update(DatabaseDocument<T> document);
}
