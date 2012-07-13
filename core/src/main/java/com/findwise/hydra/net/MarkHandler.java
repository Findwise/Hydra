package com.findwise.hydra.net;

import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.RemotePipeline;

public class MarkHandler<T extends DatabaseType> implements ResponsibleHandler {
	private enum Mark {
		PENDING, PROCESSED, DISCARDED, FAILED
	};

	private static Logger logger = LoggerFactory.getLogger(MarkHandler.class);

	DatabaseConnector<T> dbc;

	public MarkHandler(DatabaseConnector<T> dbc) {
		this.dbc = dbc;
	}

	@Override
	public void handle(HttpRequest request, HttpResponse response,
			HttpContext context) throws HttpException, IOException {
		HttpEntity requestEntity = ((HttpEntityEnclosingRequest) request)
				.getEntity();
		String requestContent = EntityUtils.toString(requestEntity);

		String stage = RESTTools.getParam(request, RemotePipeline.STAGE_PARAM);
		if (stage == null) {
			HttpResponseWriter.printMissingParameter(response, RemotePipeline.STAGE_PARAM);
			return;
		}

		DatabaseDocument<T> md;
		try {
			md = dbc.convert(new LocalDocument(requestContent));
		} catch (JsonException e) {
			HttpResponseWriter.printJsonException(response, e);
			return;
		}
		
		DatabaseDocument<T> dbdoc = dbc.getDocumentReader().getDocumentById(md.getID());
		dbdoc.putAll(md);

		if (!mark(dbdoc, stage, getMark(request))) {
			HttpResponseWriter.printNoDocument(response);
		} else {
			HttpResponseWriter.printSaveOk(response, md.getID());
		}
	}
	
	private Mark getMark(HttpRequest request) {
		String uri = RESTTools.getBaseUrl(request);
		if (uri.equals(RemotePipeline.PROCESSED_DOCUMENT_URL)) {
			return Mark.PROCESSED;
		} else if (uri.equals(RemotePipeline.PENDING_DOCUMENT_URL)) {
			return Mark.PENDING;
		} else if (uri.equals(RemotePipeline.DISCARDED_DOCUMENT_URL)) {
			return Mark.DISCARDED;
		} else if (uri.equals(RemotePipeline.FAILED_DOCUMENT_URL)) {
			return Mark.FAILED;
		}
		return null;
	}

	private boolean mark(DatabaseDocument<T> md, String stage, Mark mark) throws IOException {
		logger.trace("handleMark(..., ..., " + mark.toString() + ")");

		switch (mark) {
		case PENDING: {
			return dbc.getDocumentWriter().markPending(md, stage);
			
		}
		case PROCESSED: {
			return dbc.getDocumentWriter().markProcessed(md, stage);
		}
		case FAILED: {
			return dbc.getDocumentWriter().markFailed(md, stage);
		}
		case DISCARDED: {
			return dbc.getDocumentWriter().markDiscarded(md, stage);
		}
		}
		return false;
	}

	@Override
	public boolean supports(HttpRequest request) {
		return RESTTools.isPost(request) && getMark(request)!=null;
	}

	@Override
	public String[] getSupportedUrls() {
		return new String[] { RemotePipeline.DISCARDED_DOCUMENT_URL,
				RemotePipeline.FAILED_DOCUMENT_URL,
				RemotePipeline.PROCESSED_DOCUMENT_URL,
				RemotePipeline.PENDING_DOCUMENT_URL };
	}

}
