package com.findwise.hydra;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryCache<T extends DatabaseType> implements Cache<T> {

	private ConcurrentHashMap<DocumentID<T>, DatabaseDocument<T>> map;
	private ConcurrentHashMap<DocumentID<T>, Long> lastTouched;

	public MemoryCache() {
		map = new ConcurrentHashMap<DocumentID<T>, DatabaseDocument<T>>();
		lastTouched = new ConcurrentHashMap<DocumentID<T>, Long>();
	}

	@Override
	public void prepare() {

	}

	@Override
	public void add(DatabaseDocument<T> doc) {
		if(doc != null) {
			lastTouched.put(doc.getID(), System.currentTimeMillis());
			map.put(doc.getID(), doc);
		}
	}

	@Override
	public void add(Collection<DatabaseDocument<T>> docs) {
		for (DatabaseDocument<T> doc : docs) {
			add(doc);
		}
	}

	@Override
	public DatabaseDocument<T> remove(DocumentID<T> id) {
		lastTouched.remove(id);
		return map.remove(id);
	}

	@Override
	public Collection<DatabaseDocument<T>> removeAll() {
		List<DatabaseDocument<T>> list = new ArrayList<DatabaseDocument<T>>();

		for (DatabaseDocument<T> d : map.values()) {
			list.add(d);
		}

		map.clear();
		lastTouched.clear();

		return list;
	}

	@Override
	public DatabaseDocument<T> getDocumentById(DocumentID<T> id) {
		return map.get(id);
	}

	@Override
	public DatabaseDocument<T> getDocument() {
		return map.values().iterator().next();
	}

	@Override
	public DatabaseDocument<T> getDocument(DatabaseQuery<T> query) {
		for (Map.Entry<DocumentID<T>, DatabaseDocument<T>> entry : map
				.entrySet()) {
			if (entry.getValue().matches(query)) {
				return entry.getValue();
			}
		}
		return null;
	}

	@Override
	public ArrayList<DatabaseDocument<T>> getDocument(DatabaseQuery<T> query,
			int limit) {
		ArrayList<DatabaseDocument<T>> list = new ArrayList<DatabaseDocument<T>>();
		for (Map.Entry<DocumentID<T>, DatabaseDocument<T>> entry : map
				.entrySet()) {
			if (entry.getValue().matches(query)) {
				list.add(entry.getValue());
				if (list.size() >= limit) {
					break;
				}
			}
		}
		return list;
	}

	@Override
	public DatabaseDocument<T> getAndTag(DatabaseQuery<T> query, String ... tags) {
		for(String tag : tags) {
			query.requireNotFetchedByStage(tag);
		}
		DatabaseDocument<T> doc;
		synchronized (this) {
			doc = getDocument(query);

			if (doc != null) {
				freshen(doc.getID());
				for(String tag : tags) {
					doc.setFetchedBy(tag, new Date());
				}
			}
		}

		return doc;
	}

	@Override
	public Collection<DatabaseDocument<T>> getAndTag(DatabaseQuery<T> query,
			int n, String ... tags) {
		for(String tag : tags) {
			query.requireNotFetchedByStage(tag);
		}
		ArrayList<DatabaseDocument<T>> list;
		synchronized (this) {
			list = getDocument(query, n);

			for (DatabaseDocument<T> d : list) {
				freshen(d.getID());
				for(String tag : tags) {
					d.setFetchedBy(tag, new Date());
				}
			}
		}

		return list;
	}

	@Override
	public boolean update(DatabaseDocument<T> document) {
		DatabaseDocument<T> inCache = getDocumentById(document.getID());
		if (inCache != null) {
			inCache.putAll(document);
			freshen(inCache.getID());
			return true;
		}
		return false;
	}

	@Override
	public boolean markTouched(DocumentID<T> id, String tag) {
		DatabaseDocument<T> inCache;
		synchronized (this) {
			inCache = getDocumentById(id);
			if (inCache != null) {
				freshen(inCache.getID());
				inCache.setTouchedBy(tag, new Date());
				return true;
			}
		}
		return false;
	}

	@Override
	public int getSize() {
		return map.size();
	}

	@Override
	public Collection<DatabaseDocument<T>> removeStale(int stalerThanMs) {
		ArrayList<DatabaseDocument<T>> list = new ArrayList<DatabaseDocument<T>>();
		synchronized (this) {
			long time = System.currentTimeMillis();

			Iterator<Map.Entry<DocumentID<T>, Long>> it = lastTouched.entrySet().iterator();

			while(it.hasNext()) {
				Entry<DocumentID<T>, Long> entry = it.next();
				if (time - entry.getValue() > stalerThanMs) {
					DatabaseDocument<T> d = getDocumentById(entry.getKey());
					list.add(d);
					map.remove(d.getID());
					it.remove();
				}
			}
		}

		return list;
	}

	@Override
	public boolean freshen(DocumentID<T> id) {
		if(lastTouched.containsKey(id)) {
			lastTouched.put(id, System.currentTimeMillis());
			return true;
		}
		return false;
	}

}
