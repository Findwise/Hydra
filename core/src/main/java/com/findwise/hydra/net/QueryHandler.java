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

import com.findwise.hydra.DatabaseQuery;
import com.findwise.hydra.NodeMaster;
import com.findwise.hydra.common.Document;
import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.local.LocalQuery;
import com.findwise.hydra.local.RemotePipeline;
import com.findwise.hydra.mongodb.MongoType;
import com.findwise.hydra.net.RESTTools.Method;

public class QueryHandler implements ResponsibleHandler {

	private NodeMaster nm;

	private static Logger logger = LoggerFactory.getLogger(QueryHandler.class);

	public QueryHandler(NodeMaster nm) {
		this.nm = nm;
	}

	@Override
	public void handle(HttpRequest request, HttpResponse response,
			HttpContext arg2) throws HttpException, IOException {
		logger.trace("handleGetDocument()");
		HttpEntity requestEntity = ((HttpEntityEnclosingRequest) request)
				.getEntity();
		String requestContent = EntityUtils.toString(requestEntity);
		String stage = RESTTools.getParam(request, RemotePipeline.STAGE_PARAM);

		if (stage == null) {
			HttpResponseWriter.printMissingParameter(response,
					RemotePipeline.STAGE_PARAM);
			return;
		}

		DatabaseQuery<MongoType> dbq;
		try {
			dbq = requestToQuery(requestContent);
		} catch (JsonException e) {
			HttpResponseWriter.printJsonException(response, e);
			return;
		}

		Document d;

		String recurring = RESTTools.getParam(request,
				RemotePipeline.RECURRING_PARAM);

		if (recurring != null && recurring.equals("1")) {
			d = nm.getDatabaseConnector().getDocumentWriter()
					.getAndTagRecurring(dbq, stage);
		} else {
			d = nm.getDatabaseConnector().getDocumentWriter()
					.getAndTag(dbq, stage);
		}

		if (d != null) {
			HttpResponseWriter.printDocument(response, d, stage);
		} else {
			HttpResponseWriter.printNoDocument(response);
		}

	}

	private DatabaseQuery<MongoType> requestToQuery(String requestContent)
			throws JsonException {
		return nm.getDatabaseConnector()
				.convert(new LocalQuery(requestContent));
	}

	@Override
	public boolean supports(HttpRequest request) {
		return RESTTools.getMethod(request) == Method.POST
				&& RemotePipeline.GET_DOCUMENT_URL.equals(RESTTools
						.getBaseUrl(request));
	}

	@Override
	public String[] getSupportedUrls() {
		return new String[] { RemotePipeline.GET_DOCUMENT_URL };
	}

}
