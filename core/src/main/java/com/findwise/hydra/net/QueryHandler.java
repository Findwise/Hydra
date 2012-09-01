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
import com.findwise.hydra.DatabaseQuery;
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.StageManager;
import com.findwise.hydra.common.Document;
import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.local.LocalQuery;
import com.findwise.hydra.local.RemotePipeline;
import com.findwise.hydra.net.RESTTools.Method;

public class QueryHandler<T extends DatabaseType> implements ResponsibleHandler {
	
	private DatabaseConnector<T> dbc;

	private static Logger logger = LoggerFactory.getLogger(QueryHandler.class);

	public QueryHandler(DatabaseConnector<T> dbc) {
		this.dbc = dbc;
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

		DatabaseQuery<T> dbq;
		try {
			dbq = requestToQuery(requestContent);
		} catch (JsonException e) {
			HttpResponseWriter.printJsonException(response, e);
			return;
		}

		reportQuery(stage);		

		Document d;

		String recurring = RESTTools.getParam(request,
				RemotePipeline.RECURRING_PARAM);

		if (recurring != null && recurring.equals("1")) {
			d = dbc.getDocumentWriter().getAndTagRecurring(dbq, stage);
		} else {
			d = dbc.getDocumentWriter().getAndTag(dbq, stage);
		}

		if (d != null) {
			HttpResponseWriter.printDocument(response, d, stage);
		} else {
			HttpResponseWriter.printNoDocument(response);
		}

	}

	private DatabaseQuery<T> requestToQuery(String requestContent)
			throws JsonException {
		return dbc.convert(new LocalQuery(requestContent));
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
	
	private void reportQuery(String stage) {
		StageManager sm = StageManager.getStageManager();
		if(sm.hasRunner(stage)) {
			sm.getRunner(stage).setHasQueried();
		}
	}

}
