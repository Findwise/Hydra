package com.findwise.hydra.input;

import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.apache.http.util.EntityUtils;

import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.common.Logger;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractInputStage;
import com.findwise.hydra.stage.Parameter;
import com.findwise.hydra.stage.RequiredArgumentMissingException;
import com.findwise.hydra.stage.Stage;

/**
 * inputConfiguration={port:12002,idField:\"DREREFERENCE\"}
 * 
 * port: The port that the JsonInputStage should listen on
 * 
 * idField: The field that the input stage should consider unique, and discard
 * all documents in the pipeline with the same value of idField.
 * 
 * @author simon.stenstrom
 * 
 */
@Stage
public class JsonInputStage extends AbstractInputStage implements
		HttpRequestHandler {

	private HttpInputServer server;

	@Parameter
	private int port = HttpInputServer.DEFAULT_LISTEN_PORT;

	public void init() throws RequiredArgumentMissingException {
		Logger.info("Starting Json Input Server on port: " + this.port);
		server = new HttpInputServer(this.port, this);
		server.init();
	}

	public void handle(final HttpRequest request, final HttpResponse response,
			final HttpContext context) {
		try {
			Logger.debug("Parsing incoming request");
			HttpEntity requestEntity = ((HttpEntityEnclosingRequest) request)
					.getEntity();
			LocalDocument ld = new LocalDocument();
			String json = EntityUtils.toString(requestEntity);

			ld.fromJson(json);
			discardOld(ld);
			if (getRemotePipeline().saveFull(ld)) {
				Logger.debug("Document added");
				response.setStatusCode(200);
			} else {
				Logger.error("Document could not be added");
				response.setStatusCode(400);
			}

		} catch (JsonException e) {
			Logger.error("Json sent was malformed. Skipping document");
			response.setStatusCode(400);
		} catch (Exception e) {
			Logger.error("Unhandled exception occurred", e);
			response.setStatusCode(400);
		}
	}
}
