package com.findwise.hydra;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.DatabaseConnector.ConversionException;
import com.findwise.hydra.common.DocumentFile;

public class CachingDocumentIO<CacheType extends DatabaseType, BackingType extends DatabaseType> implements DocumentReader<CacheType>, DocumentWriter<CacheType> {
	private static final Logger logger = LoggerFactory.getLogger(CachingDocumentIO.class);

	private static final int DEFAULT_BATCH_SIZE = 10;
	
	private DocumentWriter<CacheType> cacheWriter;
	private DocumentReader<CacheType> cacheReader;
	private DocumentWriter<BackingType> backingWriter;
	private DocumentReader<BackingType> backingReader;
	private DatabaseConnector<BackingType> backingConnector;
	private DatabaseConnector<CacheType> cacheConnector;
	
	private int batchSize;

	public CachingDocumentIO(DatabaseConnector<CacheType> cacheConnector, DatabaseConnector<BackingType> backingConnector) {
		this(cacheConnector, backingConnector, DEFAULT_BATCH_SIZE);
	}
	
	public CachingDocumentIO(DatabaseConnector<CacheType> cacheConnector, DatabaseConnector<BackingType> backingConnector, int batchSize) {
		this.cacheConnector = cacheConnector;
		this.backingConnector = backingConnector;

		cacheWriter = cacheConnector.getDocumentWriter();
		cacheReader = cacheConnector.getDocumentReader();
		backingReader = backingConnector.getDocumentReader();
		backingWriter = backingConnector.getDocumentWriter();
		
		this.batchSize = batchSize;
	}
	
	private DatabaseDocument<BackingType> convert(DatabaseDocument<CacheType> d) {
		try {
			return backingConnector.convert(d);
		} catch (ConversionException e) {
			logger.error("Unable to convert document: '"+d+"' from cache to underlying type", e);
			return null;
		}
	}
	
	private DatabaseQuery<BackingType> convert(DatabaseQuery<CacheType> q) {
		return backingConnector.convert(q);
	}
	
	private DatabaseDocument<CacheType> convertToCache(DatabaseDocument<BackingType> d) {
		try {
			return cacheConnector.convert(d);
		} catch (ConversionException e) {
			logger.error("Unable to convert document: '"+d+"' from cache to underlying type", e);
			return null;
		}
	}
	
	@Override
	public DatabaseDocument<CacheType> getAndTag(
			DatabaseQuery<CacheType> query, String tag) {
		DatabaseDocument<CacheType> ret = cacheWriter.getAndTag(query, tag);
		if(ret==null) {
			fillCache(query);
			
			ret = cacheWriter.getAndTag(query, tag);
		}
		return ret;
	}

	@Override
	public DatabaseDocument<CacheType> getAndTagRecurring(
			DatabaseQuery<CacheType> query, String tag) {
		DatabaseDocument<CacheType> ret = cacheWriter.getAndTagRecurring(query, tag);
		if(ret==null) {
			fillCache(query);
			
			ret = cacheWriter.getAndTagRecurring(query, tag);
		}
		return ret;
	}

	@Override
	public DatabaseDocument<CacheType> getAndTagRecurring(
			DatabaseQuery<CacheType> query, String tag, int intervalMillis) {
		DatabaseDocument<CacheType> ret = cacheWriter.getAndTagRecurring(query, tag, intervalMillis);
		if(ret==null) {
			fillCache(query);
			
			ret = cacheWriter.getAndTagRecurring(query, tag, intervalMillis);
		}
		return ret;
	}

	@Override
	public boolean markTouched(Object id, String tag) {
		if(!cacheWriter.markTouched(id, tag)) {
			return backingWriter.markTouched(id, tag);
		}
		return true;
	}

	@Override
	public boolean markProcessed(DatabaseDocument<CacheType> d, String stage) {
		DatabaseDocument<CacheType> inCache = cacheReader.getDocumentById(d.getID());
		cacheWriter.markProcessed(d, stage);
		if(inCache==null) {
			inCache = d;
		} else {
			inCache.putAll(d);
		}
		return backingWriter.markProcessed(convert(inCache), stage);
	}

	@Override
	public boolean markDiscarded(DatabaseDocument<CacheType> d, String stage) {
		DatabaseDocument<CacheType> inCache = cacheReader.getDocumentById(d.getID());
		cacheWriter.markDiscarded(d, stage);
		if(inCache==null) {
			inCache = d;
		} else {
			inCache.putAll(d);
		}
		return backingWriter.markDiscarded(convert(inCache), stage);
	}

	@Override
	public boolean markFailed(DatabaseDocument<CacheType> d, String stage) {
		DatabaseDocument<CacheType> inCache = cacheReader.getDocumentById(d.getID());
		cacheWriter.markFailed(d, stage);
		if(inCache==null) {
			inCache = d;
		} else {
			inCache.putAll(d);
		}
		return backingWriter.markFailed(convert(inCache), stage);
	}

	@Override
	public boolean markPending(DatabaseDocument<CacheType> d, String stage) {
		if(!cacheWriter.markPending(d, stage)) {
			return backingWriter.markPending(convert(d), stage);
		}
		return true;
	}

	@Override
	public boolean insert(DatabaseDocument<CacheType> d) {
		DatabaseDocument<BackingType> doc = convert(d);
		if(doc==null) {
			return false;
		}
		boolean result = backingWriter.insert(doc);
		d.setID(convertToCache(doc).getID());
		return result;
	}

	@Override
	public boolean update(DatabaseDocument<CacheType> d) {
		if(!cacheWriter.update(d)) {
			return backingWriter.update(convert(d));
		}
		return true;
	}

	@Override
	public void delete(DatabaseDocument<CacheType> d) {
		cacheWriter.delete(d);
		backingWriter.delete(convert(d));
	}

	@Override
	public boolean deleteDocumentFile(DatabaseDocument<CacheType> d, String fileName) {
		cacheWriter.deleteDocumentFile(d, fileName);
		return backingWriter.deleteDocumentFile(convert(d), fileName);
	}

	@Override
	public void deleteAll() {
		cacheWriter.deleteAll();
		backingWriter.deleteAll();
	}

	@Override
	public void write(DocumentFile df) throws IOException {
		cacheWriter.write(df);
	}

	@Override
	public void prepare() {
		cacheWriter.prepare();
		backingWriter.prepare();
	}

	@Override
	public DatabaseDocument<CacheType> getDocument(DatabaseQuery<CacheType> q) {
		DatabaseDocument<CacheType> ret = cacheReader.getDocument(q);
		if(ret==null) {
			fillCache(q);
			return cacheReader.getDocument(q);
		}
		return ret;
	}

	@Override
	public DatabaseDocument<CacheType> getDocumentById(Object id) {
		return getDocumentById(id, false);
	}

	@Override
	public DatabaseDocument<CacheType> getDocumentById(Object id, boolean includeInactive) {
		DatabaseDocument<CacheType> ret = cacheReader.getDocumentById(id);
		if (ret == null) {
			addToCache(convertToCache(backingReader.getDocumentById(id, includeInactive)));
			ret = cacheReader.getDocumentById(id);
		}
		return ret;
	}

	@Override
	public TailableIterator<CacheType> getInactiveIterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TailableIterator<CacheType> getInactiveIterator(DatabaseQuery<CacheType> query) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<DatabaseDocument<CacheType>> getDocuments(DatabaseQuery<CacheType> q, int limit) {
		return getDocuments(q, limit, 0);
	}

	@Override
	public List<DatabaseDocument<CacheType>> getDocuments(DatabaseQuery<CacheType> q, int limit, int skip) {
		List<DatabaseDocument<CacheType>> list = cacheReader.getDocuments(q, limit, skip);
		if(list.size()<1) {
			fillCache(q);
			list = cacheReader.getDocuments(q, limit, skip);
		}
		return list;
	}

	@Override
	public long getNumberOfDocuments(DatabaseQuery<CacheType> q) {
		return cacheReader.getNumberOfDocuments(q);
	}

	@Override
	public DocumentFile getDocumentFile(DatabaseDocument<CacheType> d, String fileName) {
		DocumentFile df = cacheReader.getDocumentFile(d, fileName);
		if(df == null) {
			try {
				addToCache(backingReader.getDocumentFile(convert(d), fileName));
			} catch (IOException e) {
				logger.error("Caught IOException while trying to cache a document file: "+d, e);
			}
			df = cacheReader.getDocumentFile(d, fileName);
		}
		return df;
	}

	@Override
	public List<String> getDocumentFileNames(DatabaseDocument<CacheType> d) {
		List<String> list = cacheReader.getDocumentFileNames(d);
		list.addAll(backingReader.getDocumentFileNames(convert(d)));
		return list;
	}

	@Override
	public long getActiveDatabaseSize() {
		return cacheReader.getActiveDatabaseSize() + backingReader.getActiveDatabaseSize();
	}

	@Override
	public long getInactiveDatabaseSize() {
		return cacheReader.getInactiveDatabaseSize() + backingReader.getInactiveDatabaseSize();
	}

	@Override
	public Object toDocumentId(Object jsonPrimitive) {
		return cacheReader.toDocumentId(jsonPrimitive);
	}

	@Override
	public Object toDocumentIdFromJson(String json) {
		return cacheReader.toDocumentIdFromJson(json);
	}

	protected void fillCache(DatabaseQuery<CacheType> query) {
		for(DatabaseDocument<BackingType> dd : backingWriter.getAndTag(convert(query), "_cache", batchSize)) {
			addToCache(convertToCache(dd));
		}
	}

	protected void addToCache(DatabaseDocument<CacheType> d) {
		cacheWriter.update(d);
	}

	protected void addToCache(DocumentFile documentFile) throws IOException {
		cacheWriter.write(documentFile);
	}

	@Override
	public Collection<DatabaseDocument<CacheType>> getAndTag(DatabaseQuery<CacheType> query, String tag, int n) {
		Collection<DatabaseDocument<CacheType>> col = cacheWriter.getAndTag(query, tag, n);
		if(col.size()<n) {
			fillCache(query);
			col.addAll(cacheWriter.getAndTag(query, tag, n));
		}
		return col;
	}
}
