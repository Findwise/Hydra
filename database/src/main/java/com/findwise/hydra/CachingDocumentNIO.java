package com.findwise.hydra;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.List;

import org.slf4j.LoggerFactory;

import com.findwise.hydra.DatabaseConnector.ConversionException;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.LocalQuery;

public class CachingDocumentNIO<T extends DatabaseType> implements
		DocumentReader<T>, DocumentWriter<T> {

	public static final int DEFAULT_CACHE_TIMEOUT = 10000;
	public static final String CACHED_TIME_METADATA_KEY = "cached";
	public static final String CACHE_TAG = "_cache";

	private Cache<T> cache;

	private DatabaseConnector<T> backing;
	private DocumentWriter<T> writer;
	private DocumentReader<T> reader;

	private CacheMonitor monitor;

	private int cacheTimeout;

	private final org.slf4j.Logger logger = LoggerFactory
			.getLogger(CachingDocumentNIO.class);

	public CachingDocumentNIO(DatabaseConnector<T> backing, Cache<T> cache) {
		this(backing, cache, true);
	}

	public CachingDocumentNIO(DatabaseConnector<T> backing, Cache<T> cache,
			boolean startMonitorThread) {
		this(backing, cache, startMonitorThread, DEFAULT_CACHE_TIMEOUT);
	}

	public CachingDocumentNIO(DatabaseConnector<T> backing, Cache<T> cache,
			boolean startMonitorThread, int cacheTimeout) {
		writer = backing.getDocumentWriter();
		reader = backing.getDocumentReader();
		this.getDatabaseConnector(backing);
		this.cache = cache;
		this.cacheTimeout = cacheTimeout;

		monitor = new CacheMonitor();
		if (startMonitorThread) {
			monitor.start();
		}
	}

	@Override
	public DatabaseDocument<T> getAndTag(DatabaseQuery<T> query, String... tags) {
		DatabaseDocument<T> doc = cache.getAndTag(query, tags);

		if (doc == null) {
			for (String t : tags) {
				query.requireNotFetchedByStage(t);
			}
			doc = writer.getAndTag(query, addCacheTag(tags));
			if (doc != null) {
				for (String t : tags) {
					doc.setFetchedBy(t, new Date());
				}
				cache.add(doc);
			}
		}

		return getCopy(doc);
	}

	@Override
	public Collection<DatabaseDocument<T>> getAndTag(DatabaseQuery<T> query,
			int n, String... tags) {
		Collection<DatabaseDocument<T>> list = new ArrayList<DatabaseDocument<T>>();

		Collection<DatabaseDocument<T>> c = cache.getAndTag(query, n, tags);
		if (c != null) {
			list.addAll(c);
		}

		if (list.size() == 0) {
			for (String t : tags) {
				query.requireNotFetchedByStage(t);
			}
			list = writer.getAndTag(query, n, addCacheTag(tags));

			for (DatabaseDocument<T> d : list) {
				for (String t : tags) {
					d.setFetchedBy(t, new Date());
				}
			}
			cache.add(list);
		}

		return list;
	}

	@Override
	public boolean markTouched(DocumentID<T> id, String tag) {
		if (!cache.markTouched(id, tag)) {
			DatabaseDocument<T> d = reader.getDocumentById(id);
			if (d != null) {
				d.removeFetchedBy(CACHE_TAG);
				d.setTouchedBy(tag, new Date());
				return writer.update(d);
			}
			return false;
		}
		return true;
	}

	@Override
	public boolean markProcessed(DatabaseDocument<T> d, String stage) {
		DatabaseDocument<T> cached = cache.getDocumentById(d.getID());
		if (cached != null) {
			d.putAll(cached);
			cache.remove(d.getID());
		}
		if (writer.markProcessed(d, stage)) {
			return true;
		}
		return false;
	}

	@Override
	public boolean markDiscarded(DatabaseDocument<T> d, String stage) {
		DatabaseDocument<T> cached = cache.getDocumentById(d.getID());
		d.putAll(cached);
		cache.remove(d.getID());
		if (writer.markDiscarded(d, stage)) {
			return true;
		}
		return false;
	}

	@Override
	public boolean markFailed(DatabaseDocument<T> d, String stage) {
		DatabaseDocument<T> cached = cache.getDocumentById(d.getID());
		d.putAll(cached);
		cache.remove(d.getID());
		if (writer.markFailed(d, stage)) {
			return true;
		}
		return false;
	}

	@Override
	public boolean markPending(DatabaseDocument<T> d, String stage) {
		DatabaseDocument<T> cached = cache.getDocumentById(d.getID());
		d.putAll(cached);
		cache.remove(d.getID());
		return writer.markPending(d, stage);
	}

	@Override
	public boolean insert(DatabaseDocument<T> d) {
		return writer.insert(d);
	}

	@Override
	public boolean insert(DatabaseDocument<T> d, List<DocumentFile<T>> attachments) {
		return writer.insert(d, attachments);
	}

	@Override
	public boolean update(DatabaseDocument<T> d) {
		if (!cache.update(d)) {
			DatabaseDocument<T> doc = reader.getDocumentById(d.getID());
			if (doc != null) {
				cache.add(doc);
				cache.update(d);
			}
			if (cache.getDocumentById(d.getID()) == null) {
				d.removeFetchedBy(CACHE_TAG);
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
	public void write(DocumentFile<T> df) throws IOException {
		writer.write(df);
	}

	@Override
	public void prepare() {
		writer.prepare();
		cache.prepare();
	}

	@Override
	public DatabaseDocument<T> getDocument(DatabaseQuery<T> q) {
		DatabaseDocument<T> doc = cache.getDocument(q);

		if (doc == null) {
			doc = reader.getDocument(q);
			cache.add(doc);
		}

		return getCopy(doc);
	}

	@Override
	public DatabaseDocument<T> getDocumentById(DocumentID<T> id) {
		DatabaseDocument<T> doc = cache.getDocumentById(id);

		if (doc == null) {
			doc = reader.getDocumentById(id, false);
			if(doc != null) {
				cache.add(doc);
			} else {
				return null;
			}
		}

		return getCopy(doc);
	}

	@Override
	public DatabaseDocument<T> getDocumentById(DocumentID<T> id,
			boolean includeInactive) {
		DatabaseDocument<T> doc = getDocumentById(id);
		if (doc == null && includeInactive) {
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

	/**
	 * If there is any documents in the cache matching the query, those are
	 * returned, otherwise a direct call is made to the underlying reader.
	 * 
	 * As such, this may return 1 document (if one is found in the cache) even
	 * though there are thousands of documents in the underlying database that
	 * match.
	 */
	@Override
	public List<DatabaseDocument<T>> getDocuments(DatabaseQuery<T> q, int limit) {
		List<DatabaseDocument<T>> docs = new ArrayList<DatabaseDocument<T>>();

		docs.addAll(cache.getDocument(q, limit));

		if (docs.size() == 0) {
			docs = reader.getDocuments(q, limit);
			cache.add(docs);
		}
		return docs;
	}

	/**
	 * Since skip becomes impossible to calculate in a cached scenario, this
	 * will simply query the underlying reader.
	 */
	@Override
	public List<DatabaseDocument<T>> getDocuments(DatabaseQuery<T> q,
			int limit, int skip) {
		List<DatabaseDocument<T>> docs = reader.getDocuments(q, limit, skip);
		cache.add(docs);
		return docs;
	}

	/**
	 * Due to the nature of a cache, this method will return a result that is
	 * possibly incorrect. In the current implementation, the number reported by
	 * the underlying database is returned and the cache is ignored, since hits
	 * in the cache are potentially just replicas of the hits in the database.
	 * 
	 * We have no way of knowing what hits are missing from the reader, but are
	 * in the cache and vice versa, without some very expensive computation.
	 */
	@Override
	public long getNumberOfDocuments(DatabaseQuery<T> q) {
		return reader.getNumberOfDocuments(q);
	}

	@Override
	public DocumentFile<T> getDocumentFile(DatabaseDocument<T> d,
			String fileName) {
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
	public DocumentID<T> toDocumentId(Object jsonPrimitive) {
		return reader.toDocumentId(jsonPrimitive);
	}

	@Override
	public DocumentID<T> toDocumentIdFromJson(String json) {
		return reader.toDocumentIdFromJson(json);
	}

	public void connect() throws IOException {
		getDatabaseConnector().connect();
	}

	public DatabaseDocument<T> convert(LocalDocument localDocument)
			throws ConversionException {
		return getDatabaseConnector().convert(localDocument);
	}

	public DatabaseQuery<T> convert(LocalQuery localQuery) {
		return getDatabaseConnector().convert(localQuery);
	}

	/**
	 * Flushes the cache back to the database
	 */
	public void flush() {
		Collection<DatabaseDocument<T>> docs = cache.removeAll();
		for (DatabaseDocument<T> d : docs) {
			d.removeFetchedBy(CACHE_TAG);
			writer.update(d);
		}
	}

	/**
	 * Flushes all documents in the cache that have not been touched for the
	 * specified number of milliseconds.
	 */
	public void flush(int staleTimeout) {
		Collection<DatabaseDocument<T>> docs = cache.removeStale(staleTimeout);
		if (docs.size() > 0) {
			logger.debug("Flushing " + docs.size() + " out of "
					+ (docs.size() + cache.getSize()) + " documents from cache");
		}
		for (DatabaseDocument<T> d : docs) {
			d.removeFetchedBy(CACHE_TAG);
			writer.update(d);
		}
	}

	public void setCacheTimeout(int cacheTimeout) {
		this.cacheTimeout = cacheTimeout;
	}

	public int getCacheTimeout() {
		return cacheTimeout;
	}

	public CacheMonitor getCacheMonitor() {
		return monitor;
	}

	public DatabaseConnector<T> getDatabaseConnector() {
		return backing;
	}

	public void getDatabaseConnector(DatabaseConnector<T> backing) {
		this.backing = backing;
	}

	/**
	 * Monitor thread, responsible for upholding the Cache TTL.
	 * 
	 * Stop by interrupting.
	 */
	private class CacheMonitor extends Thread {
		CacheMonitor() {
			setDaemon(true);
			Runtime.getRuntime().addShutdownHook(new Thread() {
				public void run() {
					shutdown();
				}
			});
		}

		public void run() {
			logger.info("Starting up cache monitor thread");
			while (!isInterrupted()) {
				logger.trace("Cache size: " + cache.getSize());
				try {
					if (getCacheTimeout() != 0) {
						flush(getCacheTimeout());
					}
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					interrupt();
				}
			}
			shutdown();
		}

		private void shutdown() {

			if (isAlive()) {
				logger.info("Shutting down cache monitor thread. Attempting to save local changes.");
				flush();
			}
		}
	}

	private String[] addCacheTag(String... tags) {
		String[] s = new String[tags.length + 1];
		for (int i = 0; i < tags.length; i++) {
			s[i] = tags[i];
		}
		s[tags.length] = CACHE_TAG;

		return s;
	}

	public DatabaseDocument<T> getCopy(DatabaseDocument<T> doc) {
		if (doc == null) {
			return null;
		}
		for (int i = 0; i < 10; i++) {
			try {
				return doc.copy();
			} catch (ConcurrentModificationException e) {

			}
		}
		logger.error("Unable to copy a document with id " + doc);
		return null;
	}
}
