package com.findwise.hydra;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.findwise.hydra.DocumentFile;

public class CachingDocumentNIO<T extends DatabaseType> implements
		DocumentReader<T>, DocumentWriter<T> {

	private static final int DEFAULT_BATCH_SIZE = 10;
	private static final int DEFAULT_DOCUMENT_TTL_MS = 10000;
	public static final String CACHED_TIME_METADATA_KEY = "cached";
	public static final String CACHE_TAG = "_cache";

	private Cache<T> cache;

	private DocumentWriter<T> writer;
	private DocumentReader<T> reader;

	private int batchSize;

	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public CachingDocumentNIO(DatabaseConnector<T> backing, Cache<T> cache) {
		writer = backing.getDocumentWriter();
		reader = backing.getDocumentReader();
		this.cache = cache;
		batchSize = DEFAULT_BATCH_SIZE;
	}

	@Override
	public DatabaseDocument<T> getAndTag(DatabaseQuery<T> query, String tag) {
		DatabaseDocument<T> doc = cache.getAndTag(query, tag);
		if (doc == null) {
			if (fillCache(query, tag) > 0) {
				doc = cache.getAndTag(query, tag);
			}
		}

		return doc;
	}

	@Override
	public Collection<DatabaseDocument<T>> getAndTag(DatabaseQuery<T> query,
			String tag, int n) {
		Collection<DatabaseDocument<T>> list = new ArrayList<DatabaseDocument<T>>();

		list.addAll(cache.getAndTag(query, tag, n));

		return list;
	}

	@Override
	public boolean markTouched(DocumentID id, String tag) {
		if (!cache.markTouched(id, tag)) {
			return writer.markTouched(id, tag);
		}
		return true;
	}

	@Override
	public boolean markProcessed(DatabaseDocument<T> d, String stage) {
		if (writer.markProcessed(d, stage)) {
			cache.remove(d.getID());
			return true;
		}
		return false;
	}

	@Override
	public boolean markDiscarded(DatabaseDocument<T> d, String stage) {
		if (writer.markDiscarded(d, stage)) {
			cache.remove(d.getID());
			return true;
		}
		return false;
	}

	@Override
	public boolean markFailed(DatabaseDocument<T> d, String stage) {
		if (writer.markFailed(d, stage)) {
			cache.remove(d.getID());
			return true;
		}
		return false;
	}

	@Override
	public boolean markPending(DatabaseDocument<T> d, String stage) {
		cache.remove(d.getID());
		return writer.markPending(d, stage);
	}

	@Override
	public boolean insert(DatabaseDocument<T> d) {
		return writer.insert(d);
	}

	@Override
	public boolean update(DatabaseDocument<T> d) {
		if (!cache.update(d)) {
			DatabaseDocument<T> doc = reader.getDocumentById(d.getID());
			if (doc != null) {
				cache.add(doc);
				cache.update(d);
			}
			return writer.update(d);
		}
		return true;
	}

	@Override
	public void delete(DatabaseDocument<T> d) {
		cache.remove(d.getID());
		writer.delete(d);
	}

	@Override
	public boolean deleteDocumentFile(DatabaseDocument<T> d, String fileName) {
		return writer.deleteDocumentFile(d, fileName);
	}

	@Override
	public void deleteAll() {
		cache.removeAll();
		writer.deleteAll();
	}

	@Override
	public void write(DocumentFile df) throws IOException {
		writer.write(df);
	}

	@Override
	public void prepare() {
		cache.prepare();
		writer.prepare();
	}

	@Override
	public DatabaseDocument<T> getDocument(DatabaseQuery<T> q) {
		DatabaseDocument<T> doc = cache.getDocument(q);
		
		if(doc == null) {
			doc = reader.getDocument(q);
			cache.add(doc);
		}

		return doc;
	}

	@Override
	public DatabaseDocument<T> getDocumentById(DocumentID id) {
		DatabaseDocument<T> doc = cache.getDocumentById(id);
		
		if(doc == null) {
			doc = reader.getDocumentById(id, false);
			cache.add(doc);
		}
		
		return doc;
	}

	@Override
	public DatabaseDocument<T> getDocumentById(DocumentID id,
			boolean includeInactive) {
		DatabaseDocument<T> doc = getDocumentById(id);
		if(doc == null && includeInactive ) {
			return reader.getDocumentById(id, includeInactive);
		}
		return cache.getDocumentById(id);
	}

	@Override
	public TailableIterator<T> getInactiveIterator() {
		return reader.getInactiveIterator();
	}

	@Override
	public TailableIterator<T> getInactiveIterator(DatabaseQuery<T> query) {
		return reader.getInactiveIterator(query);
	}

	@Override
	public List<DatabaseDocument<T>> getDocuments(DatabaseQuery<T> q, int limit) {
		List<DatabaseDocument<T>> docs = new ArrayList<DatabaseDocument<T>>();
		
		docs.addAll(getDocuments(q, limit));
		
		if(docs.size() < limit) {
			return reader.getDocuments(q, limit);
		}
		return docs;
	}

	@Override
	public List<DatabaseDocument<T>> getDocuments(DatabaseQuery<T> q,
			int limit, int skip) {
		return reader.getDocuments(q, limit, skip);
	}

	@Override
	public long getNumberOfDocuments(DatabaseQuery<T> q) {
		return reader.getNumberOfDocuments(q);
	}

	@Override
	public DocumentFile getDocumentFile(DatabaseDocument<T> d, String fileName) {
		return reader.getDocumentFile(d, fileName);
	}

	@Override
	public List<String> getDocumentFileNames(DatabaseDocument<T> d) {
		return reader.getDocumentFileNames(d);
	}

	@Override
	public long getActiveDatabaseSize() {
		return reader.getActiveDatabaseSize();
	}

	@Override
	public long getInactiveDatabaseSize() {
		return reader.getInactiveDatabaseSize();
	}

	@Override
	public DocumentID toDocumentId(Object jsonPrimitive) {
		return reader.toDocumentId(jsonPrimitive);
	}

	@Override
	public DocumentID toDocumentIdFromJson(String json) {
		return reader.toDocumentIdFromJson(json);
	}

	protected int fillCache(DatabaseQuery<T> query, String tag) {
		if (tag != null) {
			query.requireNotFetchedByStage(tag);
		}

		Collection<DatabaseDocument<T>> collection = writer.getAndTag(query,
				CACHE_TAG, batchSize);

		cache.add(collection);

		return collection.size();
	}

}
