package com.findwise.hydra.admin.documents;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.DatabaseQuery;
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.admin.database.AdminServiceQuery;
import com.findwise.hydra.admin.database.AdminServiceType;
import com.findwise.hydra.common.JsonException;

public class DocumentsService<T extends DatabaseType> {

	private DatabaseConnector<T> connector;

	public DocumentsService(DatabaseConnector<T> connector) {
		this.connector = connector;
	}

	public Map<String, Object> getNumberOfDocuments(String jsonQuery) {
		Map<String, Object> ret = new HashMap<String, Object>();

		try {
			DatabaseQuery<AdminServiceType> query = new AdminServiceQuery();
			query.fromJson(jsonQuery);
			ret.put("numberOfDocuments", getNumberOfDocuments(connector.convert(query)));
		} catch (JsonException e) {
			Map<String, String> error = new HashMap<String, String>();
			error.put("Invalid query", jsonQuery);
			ret.put("error", error);
			ret.put("numberOfDocuments", 0);
		}

		return ret;
	}

	public Map<String, Object> getDocuments(String jsonQuery, int limit, int skip) {
		Map<String, Object> ret = new HashMap<String, Object>();

		try {
			DatabaseQuery<AdminServiceType> query = new AdminServiceQuery();
			query.fromJson(jsonQuery);
			ret.put("documents", getDocuments(connector.convert(query), limit, skip));
		} catch (JsonException e) {
			Map<String, String> error = new HashMap<String, String>();
			error.put("Invalid query", jsonQuery);
			ret.put("error", error);
			ret.put("documents", new ArrayList<DatabaseDocument<T>>());
		}

		return ret;
	}
	
	
	private long getNumberOfDocuments(DatabaseQuery<T> query) {
		return getConnector().getDocumentReader().getNumberOfDocuments((DatabaseQuery<T>) query);
	}

	private List<DatabaseDocument<T>> getDocuments(DatabaseQuery<T> query, int limit, int skip) {
		return getConnector().getDocumentReader().getDocuments(query, limit, skip);
	}

	public DatabaseConnector<T> getConnector() {
		return connector;
	}

}
