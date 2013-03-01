package com.findwise.hydra.memorydb;

import com.findwise.hydra.DocumentID;
import com.findwise.hydra.JsonException;
import com.findwise.hydra.local.LocalDocumentID;

public class MemoryDocumentID implements DocumentID<MemoryType> {
	
	private LocalDocumentID id;

	public MemoryDocumentID(LocalDocumentID id) {
		this.id = id;
	}

	@Override
	public Object getID() {
		return id.getID();
	}

	@Override
	public String toJSON() {
		return id.toJSON();
	}

	@Override
	public void setFromJSON(String json) throws JsonException {
		id.setFromJSON(json);
	}
	
	public LocalDocumentID getLocalDocumentID() {
		return id;
	}

	@Override
	public boolean equals(Object o) {
		return id.equals(o);
	}
	
	@Override
	public int hashCode() {
		return id.hashCode();
	}
}
