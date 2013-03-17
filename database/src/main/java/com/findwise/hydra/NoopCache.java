package com.findwise.hydra;

import java.util.Collection;
import java.util.Collections;

public class NoopCache<T extends DatabaseType> implements Cache<T> {
	
	public NoopCache() {
	}

	@Override
	public void prepare() {
		//no-op
	}

	@Override
	public void add(DatabaseDocument<T> doc) {
		//no-op
	}

	@Override
	public void add(Collection<DatabaseDocument<T>> docs) {
		//no-op
	}

	@Override
	public DatabaseDocument<T> remove(DocumentID<T> id) {
		return null;
	}

	@Override
	public Collection<DatabaseDocument<T>> removeAll() {
		return Collections.emptySet();
	}

	@Override
	public Collection<DatabaseDocument<T>> removeStale(int stalerThanMs) {
		return Collections.emptySet();
	}
	
	@Override
	public DatabaseDocument<T> getDocumentById(DocumentID<T> id) {
		return null;
	}

	@Override
	public DatabaseDocument<T> getDocument() {
		return null;
	}

	@Override
	public DatabaseDocument<T> getDocument(DatabaseQuery<T> query) {
		return null;
	}

	@Override
	public Collection<DatabaseDocument<T>> getDocument(DatabaseQuery<T> query,
			int limit) {
		return Collections.emptySet();
	}

	@Override
	public DatabaseDocument<T> getAndTag(DatabaseQuery<T> query, String ... tags) {
		return null;
	}

	@Override
	public Collection<DatabaseDocument<T>> getAndTag(DatabaseQuery<T> query,
			int n, String ... tags) {
		return Collections.emptySet();
	}

	@Override
	public boolean markTouched(DocumentID<T> id, String tag) {
		return false;
	}

	@Override
	public boolean update(DatabaseDocument<T> document) {
		return false;
	}

	@Override
	public int getSize() {
		return 0;
	}

	@Override
	public boolean freshen(DocumentID<T> id) {
		return false;
	}
}
