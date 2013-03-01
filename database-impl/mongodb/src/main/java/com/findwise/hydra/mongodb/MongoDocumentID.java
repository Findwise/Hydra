package com.findwise.hydra.mongodb;

import java.util.Map;

import org.bson.types.ObjectId;

import com.findwise.hydra.DocumentID;
import com.findwise.hydra.JsonException;
import com.findwise.hydra.SerializationUtils;

public class MongoDocumentID implements DocumentID<MongoType> {
	private ObjectId id;

	public MongoDocumentID(ObjectId id) {
		this.id = id;
	}

	@Override
	public ObjectId getID() {
		return id;
	}

	@Override
	public String toJSON() {
		return SerializationUtils.toJson(id);
	}

	@Override
	public void setFromJSON(String json) throws JsonException {
		id = toObjectId(SerializationUtils.fromJson(json));

	}

	public static MongoDocumentID getDocumentID(String json)
			throws JsonException {
		return getDocumentID(SerializationUtils.fromJson(json));
	}

	public static MongoDocumentID getDocumentID(Map<String, Object> map)
			throws JsonException {
		ObjectId id = toObjectId(map);
		if (id == null) {
			return null;
		}
		return new MongoDocumentID(id);
	}
	
	public static ObjectId getObjectId(String json) throws JsonException {
		return toObjectId(SerializationUtils.fromJson(json));
	}

	protected static ObjectId toObjectId(Map<String, Object> m) {
		if (m.containsKey("_time") && m.containsKey("_machine")
				&& m.containsKey("_inc")) {
			return new ObjectId((Integer) m.get("_time"),
					(Integer) m.get("_machine"), (Integer) m.get("_inc"));
		}
		return null;
	}
	
	@Override
	public String toString() {
		return toJSON();
	}
}
