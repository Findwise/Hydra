package com.findwise.hydra;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.DatabaseConnector.ConversionException;
import com.findwise.hydra.common.DocumentFile;

public class CachingDocumentIO<CacheType extends DatabaseType, BackingType extends DatabaseType> implements DocumentReader<CacheType>, DocumentWriter<CacheType> {
	private static final Logger logger = LoggerFactory.getLogger(CachingDocumentIO.class);
	
	private DocumentWriter<CacheType> cacheWriter;
	private DocumentReader<CacheType> cacheReader;
	private DocumentWriter<BackingType> backingWriter;
	private DocumentReader<BackingType> backingReader;
	private DatabaseConnector<BackingType> backingConnector;
	private DatabaseConnector<CacheType> cacheConnector;
	
	public CachingDocumentIO(DatabaseConnector<CacheType> cacheConnector, DatabaseConnector<BackingType> backingConnector) {
		this.cacheConnector = cacheConnector;
		this.backingConnector = backingConnector;
		this.cacheWriter = cacheConnector.getDocumentWriter();
		this.cacheReader = cacheConnector.getDocumentReader();
		this.backingReader = backingConnector.getDocumentReader();
		this.backingWriter = backingConnector.getDocumentWriter();
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
	
	private DatabaseQuery<CacheType> converToCache(DatabaseQuery<BackingType> q) {
		return cacheConnector.convert(q);
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
		if(!cacheWriter.markProcessed(d, stage)) {
			return backingWriter.markProcessed(convert(d), stage);
		}
		return true;
	}

	@Override
	public boolean markDiscarded(DatabaseDocument<CacheType> d, String stage) {
		if(!cacheWriter.markDiscarded(d, stage)) {
			return backingWriter.markDiscarded(convert(d), stage);
		}
		return true;
	}

	@Override
	public boolean markFailed(DatabaseDocument<CacheType> d, String stage) {
		if(!cacheWriter.markFailed(d, stage)) {
			return backingWriter.markFailed(convert(d), stage);
		}
		return true;
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
		return backingWriter.insert(doc);
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
			if(includeInactive) {
				addToCache(convertToCache(backingReader.getDocumentById(id, includeInactive)));
			} else {
				return null;
			}
			return cacheReader.getDocumentById(id);
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
		List<DatabaseDocument<CacheType>> list = cacheReader.getDocuments(q, limit);
		if(list.size()<1) {
			
		}
	}

	@Override
	public List<DatabaseDocument<CacheType>> getDocuments(
			DatabaseQuery<CacheType> q, int limit, int skip) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getNumberOfDocuments(DatabaseQuery<CacheType> q) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public DocumentFile getDocumentFile(DatabaseDocument<CacheType> d,
			String fileName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getDocumentFileNames(DatabaseDocument<CacheType> d) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getActiveDatabaseSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getInactiveDatabaseSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Object toDocumentId(Object jsonPrimitive) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object toDocumentIdFromJson(String json) {
		// TODO Auto-generated method stub
		return null;
	}

	private void fillCache(DatabaseQuery<CacheType> query) {
		// TODO Auto-generated method stub
		
	}

	private void addToCache(DatabaseDocument<CacheType> convertToCache) {
		// TODO Auto-generated method stub
		
	}
}
