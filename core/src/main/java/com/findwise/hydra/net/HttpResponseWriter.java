package com.findwise.hydra.net;

import java.io.UnsupportedEncodingException;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.nio.entity.NStringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.common.Document;
import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.common.SerializationUtils;

/**
 * This class provides methods for writing output to a HttpResponse.
 * 
 * @author joel.westberg
 */
public final class HttpResponseWriter {
	private static Logger logger = LoggerFactory.getLogger(HttpResponseWriter.class);

	private static final String DEFAULT_ENCODING = "UTF-8";
	private static final String DEFAULT_CONTENT_TYPE = "text/html; charset=UTF-8";

	private HttpResponseWriter() {} // Should not be possible to instantiate

	private static void setStringEntity(HttpResponse response, String content) {
		try {
			NStringEntity entity = new NStringEntity(content, DEFAULT_ENCODING);
			entity.setContentType(DEFAULT_CONTENT_TYPE);
			response.setEntity(entity);
		}
		catch (UnsupportedEncodingException e2) {
			logger.error("Encoding exception", e2);
		}
	}
	
	protected static void printDocument(HttpResponse response, Document d, String stage) {
		logger.debug("Printing document with ID " + d.getID() + " to stage " + stage);
		response.setStatusCode(HttpStatus.SC_OK);
		setStringEntity(response, d.toJson());
	}

	protected static void printDocumentReleased(HttpResponse response) {
		logger.debug("Printing release successful");
		response.setStatusCode(HttpStatus.SC_OK);
		setStringEntity(response, "Document successfully released");
	}

	protected static void printNoDocument(HttpResponse response) {
		logger.debug("Printing no document found");
		response.setStatusCode(HttpStatus.SC_NOT_FOUND);
		setStringEntity(response, "No document found matching your query");
	}
	
	protected static void printUpdateFailed(HttpResponse response, Object id) {
		logger.debug("Printing updateFailed for id:"+id);
		response.setStatusCode(HttpStatus.SC_NOT_FOUND);
		setStringEntity(response, "Update of your document failed. Sent ID { _id : '"+id+"' } probably doesn't exist.");
	}

	protected static void printDeadNode(HttpResponse response) {
		logger.debug("Printing node is dead");
		response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
		setStringEntity(response, "Node appears to be dead");
	}

	protected static void printNotPost(HttpResponse response) {
		logger.debug("Received bad request (not post)");
		response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
		setStringEntity(response, "Use POST to talk to the Node");
	}

	protected static void printJsonException(HttpResponse response, JsonException e) {
		logger.error("Received bad JSON");
		response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
		setStringEntity(response, "Unable to parse query: " + e.getMessage());
	}

	protected static void printUnsupportedRequest(HttpResponse response) {
		logger.error("Request not understood");
		response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
		setStringEntity(response, "Unimplemented action");
	}

	protected static void printMissingID(HttpResponse response) {
		logger.error("Submitted document was missing the required ID field");
		response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
		setStringEntity(response, "Submitted document was missing the required ID field");
	}

	protected static void printMissingParameter(HttpResponse response, String param) {
		logger.error(param + " parameter is missing from request");
		response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
		setStringEntity(response, "Parameter '" + param + "' is missing from request URI");
	}

	protected static void printSaveOk(HttpResponse response, Object id) {
		logger.debug("Successfully saved Document with ID: " + id);
		response.setStatusCode(HttpStatus.SC_OK);
		setStringEntity(response, "Document " + id + " successfully saved");
	}
	
	protected static void printFileDeleteOk(HttpResponse response, String filename, Object id) {
		logger.debug("Successfully deleted file with filename: "+filename+" from document " + id);
		response.setStatusCode(HttpStatus.SC_OK);
		setStringEntity(response, "File " + id + " successfully deleted");
	}

	protected static void printInsertOk(HttpResponse response, Document d) {
		logger.debug("Successfully inserted a document with ID: " + d.getID());
		response.setStatusCode(HttpStatus.SC_OK);
		setStringEntity(response, d.contentFieldsToJson(null));
	}

	protected static void printInsertFailed(HttpResponse response) {
		logger.error("Failed to insert the document");
		response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
		setStringEntity(response, "An error occurred while inserting the document");
	}

	protected static void printReleaseFailed(HttpResponse response) {
		logger.error("Failed to release the document");
		response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
		setStringEntity(response, "An error occurred while releasing the document");
	}

	protected static void printUnhandledException(HttpResponse response, Exception e) {
		logger.error("Printing Unhandled Exception");
		response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
		setStringEntity(response, "An internal server error occurred with the message " + e.getMessage());
	}
	
	protected static void printBadRequestContent(HttpResponse response) {
		logger.error("Printing Bad Request Content");
		response.setStatusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
		setStringEntity(response, "Bad Request content. Content not understood.");
	}
	
	protected static void printJson(HttpResponse response, Object o) {
		logger.debug("Printing a JSON response.");
		response.setStatusCode(HttpStatus.SC_OK);
		setStringEntity(response, SerializationUtils.toJson(o));
	}
	

	protected static void printID(HttpResponse response, String uuid) {
		logger.info("Got ID ping!");
		response.setStatusCode(HttpStatus.SC_OK);
		setStringEntity(response, uuid);
	}

	protected static void printAccessDenied(HttpResponse response) {
		logger.warn("Denying access");
		response.setStatusCode(HttpStatus.SC_FORBIDDEN);
		setStringEntity(response, "Access forbidden");
	}

	protected static void printFileNotFound(HttpResponse response, String fileName) {
		logger.debug("Printing no file found");
		response.setStatusCode(HttpStatus.SC_NOT_FOUND);
		setStringEntity(response, "No file found by the name "+fileName+" for this document");
	}
}
