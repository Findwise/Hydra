package com.findwise.hydra;

import java.io.IOException;

public class CachingDatabaseConnector<BackingType extends DatabaseType>
		implements DatabaseConnector<BackingType> {
	private DatabaseConnector<BackingType> backing;
	private Cache<BackingType> cache;
	private CachingDocumentNIO<BackingType> documentio;

	public CachingDatabaseConnector(DatabaseConnector<BackingType> backing,
			Cache<BackingType> cache) {
		this.backing = backing;
		this.cache = cache;

	}

	@Override
	public void connect() throws IOException {
		backing.connect();
		documentio = new CachingDocumentNIO<BackingType>(backing, cache);
		documentio.prepare();
	}

	@Override
	public void waitForWrites(boolean alwaysBlocking) {
		backing.waitForWrites(alwaysBlocking);
	}

	@Override
	public boolean isWaitingForWrites() {
		return backing.isWaitingForWrites();
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
	public DocumentReader<BackingType> getDocumentReader() {
		return documentio;
	}

	@Override
	public DocumentWriter<BackingType> getDocumentWriter() {
		return documentio;
	}

	@Override
	public DatabaseQuery<BackingType> convert(Query query) {
		return backing.convert(query);
	}

	@Override
	public DatabaseDocument<BackingType> convert(Document<?> document)
			throws ConversionException {
		return backing.convert(document);
	}

	@Override
	public boolean isConnected() {
		return backing.isConnected();
	}

	@Override
	public StatusWriter<BackingType> getStatusWriter() {
		return backing.getStatusWriter();
	}

	@Override
	public StatusReader<BackingType> getStatusReader() {
		return backing.getStatusReader();
	}

}
