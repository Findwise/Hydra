package com.findwise.hydra;

public interface DocumentID<Type> {
	public Object getID();
	
	public String toJSON();
	
	public void setFromJSON(String json) throws JsonException;
}
