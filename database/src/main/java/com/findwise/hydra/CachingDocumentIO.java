package com.findwise.hydra;

import java.io.IOException;
import java.util.List;

import com.findwise.hydra.common.DocumentFile;

public class CachingDocumentIO<CacheType extends DatabaseType, BackingType extends DatabaseType> implements DocumentReader<CacheType>, DocumentWriter<CacheType> {

	private DocumentWriter<CacheType> cacheWriter;
	private DocumentReader<CacheType> cacheReader;
	private DocumentWriter<BackingType> backingWriter;
	private DocumentReader<BackingType> backingReader;
	
	public CachingDocumentIO(DocumentWriter<CacheType> cacheWriter, DocumentReader<CacheType> cacheReader, DocumentWriter<BackingType> backingWriter, DocumentReader<BackingType> backingReader) {
		this.cacheWriter = cacheWriter;
		this.cacheReader = cacheReader;
		this.backingReader = backingReader;
		this.backingWriter = backingWriter;
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DatabaseDocument<CacheType> getAndTagRecurring(
			DatabaseQuery<CacheType> query, String tag, int intervalMillis) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean markTouched(Object id, String tag) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean markProcessed(DatabaseDocument<CacheType> d, String stage) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean markDiscarded(DatabaseDocument<CacheType> d, String stage) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean markFailed(DatabaseDocument<CacheType> d, String stage) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean markPending(DatabaseDocument<CacheType> d, String stage) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean insert(DatabaseDocument<CacheType> d) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean update(DatabaseDocument<CacheType> d) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void delete(DatabaseDocument<CacheType> d) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean deleteDocumentFile(DatabaseDocument<CacheType> d,
			String fileName) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void deleteAll() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DocumentFile df) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void prepare() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public DatabaseDocument<CacheType> getDocument(DatabaseQuery<CacheType> q) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DatabaseDocument<CacheType> getDocumentById(Object id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DatabaseDocument<CacheType> getDocumentById(Object id,
			boolean includeInactive) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TailableIterator<CacheType> getInactiveIterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TailableIterator<CacheType> getInactiveIterator(
			DatabaseQuery<CacheType> query) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<DatabaseDocument<CacheType>> getDocuments(
			DatabaseQuery<CacheType> q, int limit) {
		// TODO Auto-generated method stub
		return null;
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
}
