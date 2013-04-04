package com.findwise.hydra.local;

import com.findwise.hydra.DocumentID;
import com.findwise.hydra.JsonException;
import com.findwise.hydra.SerializationUtils;

public class LocalDocumentID implements DocumentID<Local> {
	private Object id;
	
	public LocalDocumentID(Object id) {
		this.id = id;
	}
	
	public Object getID() {
		return id;
	}
	
	public boolean equals(DocumentID<?> id) {
		return this.id.equals(id.getID());
	}

	@Override
	public String toJSON() {
		return SerializationUtils.toJson(id);
	}

	@Override
	public void setFromJSON(String json) throws JsonException {
		id = SerializationUtils.fromJson(json);
	}
	
	public static LocalDocumentID getDocumentID(String json)
			throws JsonException {
		return new LocalDocumentID(SerializationUtils.toObject(json));
	}
	
	@Override
	public boolean equals(Object o) {
		if(id==null) {
			return o == null;
		}
		if(o instanceof DocumentID || DocumentID.class.isAssignableFrom(o.getClass())) {
			return id.equals(((DocumentID<?>) o).getID());
		}
		return id.equals(o);
	}
	
	@Override
	public int hashCode() {
		return id.hashCode();
	}
	
	@Override
	public String toString() {
		return toJSON();
	}
}
