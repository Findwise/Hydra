package com.findwise.hydra;

import java.io.IOException;

import com.findwise.hydra.common.Document;
import com.findwise.hydra.common.Query;

public class CachingDatabaseConnector<BackingType extends DatabaseType, CacheType extends DatabaseType>
		implements DatabaseConnector<CacheType> {
	private DatabaseConnector<BackingType> backing;
	private DatabaseConnector<CacheType> cache;
	private CachingDocumentIO<CacheType, BackingType> documentio;

	public CachingDatabaseConnector(DatabaseConnector<BackingType> backing,
			DatabaseConnector<CacheType> cache) {
		this.backing = backing;
		this.cache = cache;

	}

	@Override
	public void connect() throws IOException {
		backing.connect();
		cache.connect();
		documentio = new CachingDocumentIO<CacheType, BackingType>(cache, backing);
	}

	@Override
	public void waitForWrites(boolean alwaysBlocking) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isWaitingForWrites() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public PipelineReader getPipelineReader() {
		return backing.getPipelineReader();
	}

	@Override
	public PipelineWriter getPipelineWriter() {
		return backing.getPipelineWriter();
	}

	@Override
	public DocumentReader<CacheType> getDocumentReader() {
		return documentio;
	}

	@Override
	public DocumentWriter<CacheType> getDocumentWriter() {
		return documentio;
	}

	@Override
	public DatabaseQuery<CacheType> convert(Query query) {
		return cache.convert(query);
	}

	@Override
	public DatabaseDocument<CacheType> convert(Document document)
			throws ConversionException {
		return cache.convert(document);
	}

	@Override
	public boolean isConnected() {
		return backing.isConnected() && cache.isConnected();
	}

	@Override
	public StatusWriter<CacheType> getStatusWriter() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StatusReader<CacheType> getStatusReader() {
		// TODO Auto-generated method stub
		return null;
	}

}
