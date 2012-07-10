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
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.common.Document;
import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.RemotePipeline;
import com.findwise.hydra.net.RESTTools.Method;

public class ReleaseHandler<T extends DatabaseType> implements ResponsibleHandler {

	private DatabaseConnector<T> dbc;

	private static Logger logger = LoggerFactory
			.getLogger(ReleaseHandler.class);

	public ReleaseHandler(DatabaseConnector<T> dbc) {
		this.dbc = dbc;
	}

	@Override
	public void handle(HttpRequest request, HttpResponse response,
			HttpContext arg2) throws HttpException, IOException {
		logger.trace("handleReleaseDocument()");
		HttpEntity requestEntity = ((HttpEntityEnclosingRequest) request)
				.getEntity();
		String requestContent = EntityUtils.toString(requestEntity);
		String stage = RESTTools.getParam(request, RemotePipeline.STAGE_PARAM);

		if (stage == null) {
			HttpResponseWriter.printMissingParameter(response,
					RemotePipeline.STAGE_PARAM);
			return;
		}

		try {
			boolean x = release(new LocalDocument(requestContent), stage);
			if (!x) {
				HttpResponseWriter.printNoDocument(response);
			}
		} catch (JsonException e) {
			HttpResponseWriter.printJsonException(response, e);
			return;
		}

		HttpResponseWriter.printDocumentReleased(response);

	}

	private boolean release(Document md, String stage) {
		return dbc.getDocumentWriter().markTouched(md.getID(), stage);
	}

	@Override
	public boolean supports(HttpRequest request) {
		return RESTTools.getMethod(request) == Method.POST
				&& RemotePipeline.RELEASE_DOCUMENT_URL.equals(RESTTools
						.getBaseUrl(request));
	}

	@Override
	public String[] getSupportedUrls() {
		return new String[] { RemotePipeline.RELEASE_DOCUMENT_URL };
	}

}
