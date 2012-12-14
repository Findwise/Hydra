package com.findwise.hydra.memorydb;

import java.io.IOException;

import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.DatabaseQuery;
import com.findwise.hydra.DocumentReader;
import com.findwise.hydra.DocumentWriter;
import com.findwise.hydra.PipelineReader;
import com.findwise.hydra.PipelineWriter;
import com.findwise.hydra.StatusReader;
import com.findwise.hydra.StatusWriter;
import com.findwise.hydra.common.Document;
import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.common.Query;

public class MemoryConnector implements DatabaseConnector<MemoryType> {

	private MemoryDocumentIO docio;
	private MemoryStatusIO statusio;
	
	public MemoryConnector() {
		docio = new MemoryDocumentIO();
	}
	
	@Override
	public void connect() throws IOException {
		
	}
	
	@Override
	public void waitForWrites(boolean alwaysBlocking) {
		
	}

	@Override
	public boolean isWaitingForWrites() {
		return true;
	}

	@Override
	public PipelineReader getPipelineReader() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PipelineWriter getPipelineWriter() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DocumentReader<MemoryType> getDocumentReader() {
		return docio;
	}

	@Override
	public DocumentWriter<MemoryType> getDocumentWriter() {
		return docio;
	}

	@Override
	public DatabaseQuery<MemoryType> convert(Query query) {
		MemoryQuery mq = new MemoryQuery();
		try {
			mq.fromJson(query.toJson());
		} catch (JsonException e) {
			e.printStackTrace();
		}
		return mq;
	}

	@Override
	public DatabaseDocument<MemoryType> convert(Document document) {
		MemoryDocument md = new MemoryDocument();
		try {
			md.fromJson(document.toJson());
		} catch (JsonException e) {
			e.printStackTrace();
		}
		return md;
	}
	
	@Override
	public boolean isConnected() {
		return true;
	}

	@Override
	public StatusWriter<MemoryType> getStatusWriter() {
		return statusio;
	}

	@Override
	public StatusReader<MemoryType> getStatusReader() {
		return statusio;
	}


}
