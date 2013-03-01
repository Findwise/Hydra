package com.findwise.hydra.admin.documents;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.DatabaseConnector.ConversionException;
import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.DatabaseQuery;
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.Document;
import com.findwise.hydra.JsonException;
import com.findwise.hydra.SerializationUtils;
import com.findwise.hydra.admin.database.AdminServiceQuery;
import com.findwise.hydra.admin.database.AdminServiceType;
import com.findwise.hydra.local.LocalDocument;

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

	public Map<String, Object> updateDocuments(String jsonQuery, int limit,
			String changes) {

		Map<String, Object> ret = new HashMap<String, Object>();
		try {
			DatabaseQuery<AdminServiceType> query = new AdminServiceQuery();
			query.fromJson(jsonQuery);
			List<DatabaseDocument<T>> documents = getDocuments(
					connector.convert(query), limit, 0);

			if (!documents.isEmpty()) {

				try {
					Map<String, Object> changesMap = SerializationUtils
							.fromJson(changes);
					Set<DatabaseDocument<T>> changedDocuments = new HashSet<DatabaseDocument<T>>();

					@SuppressWarnings("unchecked")
					Map<String, List<String>> deleteObject = (Map<String, List<String>>) changesMap
							.get("deletes");

					if (deleteObject != null) {
						for (DatabaseDocument<T> document : documents) {
							List<String> fetched = deleteObject.get("fetched");
							if (fetched != null) {
								for (String field : fetched) {
									boolean change = document
											.removeFetchedBy(field);
									if (change) {
										connector.getDocumentWriter().update(
												document);
										changedDocuments.add(document);
									}
								}
							}
							List<String> touched = deleteObject.get("touched");
							if (touched != null) {
								for (String field : touched) {
									boolean change = document
											.removeTouchedBy(field);
									if (change) {
										connector.getDocumentWriter().update(
												document);
										changedDocuments.add(document);
									}
								}
							}
						}
					}

					ret.put("numberOfChangedDocuments", changedDocuments.size());
					ret.put("changedDocuments", changedDocuments);
				} catch (ClassCastException e) {
					Map<String, String> error = new HashMap<String, String>();
					error.put("Invalid change map", changes);
					error.put("Expected format:",
							"{\"deletes\":{fetched:[\"staticField\"]},touched:[\"staticField\"]}");
					ret.put("error", error);
					ret.put("numberOfChangedDocuments", 0);
				} catch (JsonException e) {
					Map<String, String> error = new HashMap<String, String>();
					error.put("Invalid change map", changes);
					error.put("Expected format:",
							"{\"deletes\":{fetched:[\"staticField\"]},touched:[\"staticField\"]}");
					ret.put("error", error);
					ret.put("numberOfChangedDocuments", 0);
				}

			}

		} catch (JsonException e) {

			Map<String, String> error = new HashMap<String, String>();
			error.put("Invalid query", jsonQuery);
			ret.put("error", error);
			ret.put("numberOfChangedDocuments", 0);
		}

		return ret;
	}

	public Map<String, Object> putDocument(String action, String contentAsJson) {
		Map<String, Object> ret = new HashMap<String, Object>();
		LocalDocument doc = new LocalDocument();
		
		try {
			doc.setAction(Document.Action.valueOf(action));
			Map<String, Object> content = SerializationUtils.fromJson(contentAsJson);
			doc.getContentMap().putAll(content);
			
			DatabaseDocument<T> dbDoc = connector.convert(doc);
			
			boolean success = connector.getDocumentWriter().insert(dbDoc);
			ret.put("success", success);
		} catch (IllegalArgumentException e) {
			Map<String, Object> error = new HashMap<String, Object>();
			error.put("Invalid action", action);
			error.put("Allowed values", Document.Action.values());
			ret.put("error", error);
		} catch (JsonException e) {
			Map<String, Object> error = new HashMap<String, Object>();
			error.put("Invalid JSON document", contentAsJson);
			error.put("Expected format:", "{\"fieldname\":\"value\",\"otherfield\":\"otherfieldvalue\"}");
			ret.put("error", error);
		} catch (ConversionException e) {
			Map<String, Object> error = new HashMap<String, Object>();
			error.put("Could not convert to database document", e);
			ret.put("error", error);
		}
		
		return ret;
	}
}
