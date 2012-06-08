package com.findwise.hydra.net;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.DatabaseQuery;
import com.findwise.hydra.NodeMaster;
import com.findwise.hydra.common.Document;
import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.LocalQuery;
import com.findwise.hydra.local.RemotePipeline;
import com.findwise.hydra.mongodb.MongoType;
import com.google.inject.Inject;

public class HttpRESTHandler implements HttpRequestHandler  {
	private Logger logger = LoggerFactory.getLogger(HttpRESTHandler.class);
	
	private NodeMaster nm;
	
	private String restId;
	
	@Inject
    public HttpRESTHandler(NodeMaster nm) {
        this.nm = nm;
    }
	
	public void setRestId(String restId) {
		this.restId = restId;
	}

	public void handle(final HttpRequest request, final HttpResponse response, final HttpContext context) {
		try {
			logger.debug("Parsing incoming request");
			
			if(handleIfPing(request, response)) {
				return;
			}

			if (!nm.isAlive()) {
				HttpResponseWriter.printDeadNode(response);
				return;
			}
			
			if(handleIfGet(request, response)) {
				return;
			}
			
			if(handleIfPost(request, response)) {
				return;
			}
			
			if(!handleSupportedRequests(request, response)) {
				HttpResponseWriter.printUnsupportedRequest(response);
			}
		} catch (Exception e) {
			logger.error("Unhandled exception occurred", e);
			HttpResponseWriter.printUnhandledException(response, e);
			System.exit(1);
		}
    }
	
	private boolean handleSupportedRequests(HttpRequest request, HttpResponse response) throws IOException {
		String uri = getUri(request).substring(1);
		logger.debug("handleSupportedRequests() uri: "+uri);
		if (uri.startsWith(RemotePipeline.GET_DOCUMENT_URL)) {
			handleGetDocument(request, response);
			return true;
		} else if (uri.startsWith(RemotePipeline.RELEASE_DOCUMENT_URL)) {
			handleReleaseDocument(request, response);
			return true;
		} else if (uri.startsWith(RemotePipeline.WRITE_DOCUMENT_URL)) {
			handleWriteDocument(request, response);
			return true;
		} else if (uri.startsWith(RemotePipeline.PROCESSED_DOCUMENT_URL)) {
			handleProcessedDocument(request, response);
			return true;
		} else if (uri.startsWith(RemotePipeline.PENDING_DOCUMENT_URL)) {
			handlePendingDocument(request, response);
			return true;
		} else if (uri.startsWith(RemotePipeline.DISCARDED_DOCUMENT_URL)) {
			handleDiscardedRequest(request, response);
			return true;
		}
		
		return false;
	}
	
	private boolean handleIfGet(HttpRequest request, HttpResponse response) throws IOException {
		String method = request.getRequestLine().getMethod().toUpperCase(Locale.ENGLISH);
		if (!method.equals("GET")) {
			return false;
		}
		String uri = getUri(request).substring(1);
		if(uri.startsWith(RemotePipeline.GET_PROPERTIES_URL)) {
			handleGetProperties(request, response);
			return true;
		}
		return false;
	}
	
	private boolean handleIfPing(HttpRequest request, HttpResponse response) {
		String method = request.getRequestLine().getMethod().toUpperCase(Locale.ENGLISH);
		if (method.equals("GET")) {
			String uri = getUri(request).substring(1);
			if (uri.equals("")) {
				HttpResponseWriter.printID(response, restId);
				return true;
			}
		}
		return false;
	}
	
	private boolean handleIfPost(HttpRequest request, HttpResponse response) {
		if (!(request instanceof HttpEntityEnclosingRequest)) {
			HttpResponseWriter.printNotPost(response);
			return true;
		}
		return false;
	}
	
	private void handleGetProperties(HttpRequest request, HttpResponse response) throws IOException {
		logger.trace("handleGetProperties()");
        String stage = getParam(request, RemotePipeline.STAGE_PARAM);
        logger.debug("Received getProperties()-request for stage: "+stage);
        
        if(stage==null) {
        	HttpResponseWriter.printMissingParameter(response, RemotePipeline.STAGE_PARAM);
        	return;
        }
        
        Map<String, Object> map = new HashMap<String, Object>();
        
        if(nm.getPipeline().hasStage(stage)) {
        	map = nm.getPipeline().getStage(stage).getProperties();
        }
        
        if(handleGetDebugProperties()) {
        	return;
        }
        
        HttpResponseWriter.printJson(response, map);
	}
	
	private boolean handleGetDebugProperties() {
		return false;
	}
	
	private void handleGetDocument(HttpRequest request, HttpResponse response) throws IOException {
		logger.trace("handleGetDocument()");
        HttpEntity requestEntity = ((HttpEntityEnclosingRequest) request).getEntity();
        String requestContent = EntityUtils.toString(requestEntity);
        String stage = getParam(request, RemotePipeline.STAGE_PARAM);
        
        if(stage==null) {
        	HttpResponseWriter.printMissingParameter(response, RemotePipeline.STAGE_PARAM);
        	return;
        }
        
        DatabaseQuery<MongoType> dbq;
        try {
        	 dbq = requestToQuery(requestContent);
        } 
        catch(JsonException e) {
        	HttpResponseWriter.printJsonException(response, e);
        	return;
        }
        
        Document d;

        String recurring = getParam(request, RemotePipeline.RECURRING_PARAM);

        if(recurring!=null && recurring.equals("1")) {
        	d = nm.getDatabaseConnector().getDocumentWriter().getAndTagRecurring(dbq, stage);
        }
        else {
        	d = nm.getDatabaseConnector().getDocumentWriter().getAndTag(dbq, stage);
        }
        
        if(d!=null) {
        	HttpResponseWriter.printDocument(response, d, stage);
        }
        else {
        	HttpResponseWriter.printNoDocument(response);
        }
	}
	
	private void handleReleaseDocument(HttpRequest request, HttpResponse response) throws IOException {
		logger.trace("handleReleaseDocument()");
        HttpEntity requestEntity = ((HttpEntityEnclosingRequest) request).getEntity();
        String requestContent = EntityUtils.toString(requestEntity);
        String stage = getParam(request, RemotePipeline.STAGE_PARAM);
        
        if(stage==null) {
        	HttpResponseWriter.printMissingParameter(response, RemotePipeline.STAGE_PARAM);
        	return;
        }
        
        try {
        	boolean x = release(new LocalDocument(requestContent), stage);
        	if(!x) {
        		HttpResponseWriter.printNoDocument(response);
        	}
        }
        catch(JsonException e) {
        	HttpResponseWriter.printJsonException(response, e);
        	return;
        }
        
        HttpResponseWriter.printDocumentReleased(response);
	}
	
	private boolean release(Document md, String stage) {
		return nm.getDatabaseConnector().getDocumentWriter().markTouched(md.getID(),stage);
	}
	
	private void handleProcessedDocument(HttpRequest request, HttpResponse response) throws IOException {
		logger.trace("handleProcessedDocument()");
        HttpEntity requestEntity = ((HttpEntityEnclosingRequest) request).getEntity();
        String requestContent = EntityUtils.toString(requestEntity);
        
        String stage = getParam(request, RemotePipeline.STAGE_PARAM);
        if(stage==null) {
        	HttpResponseWriter.printMissingParameter(response, RemotePipeline.STAGE_PARAM);
        	return;
        }
        
        DatabaseDocument<MongoType> md;
        try {
        	md = nm.getDatabaseConnector().convert(new LocalDocument(requestContent));
        }
        catch(JsonException e) {
        	HttpResponseWriter.printJsonException(response, e);
        	return;
        }
        
        boolean res = nm.getDatabaseConnector().getDocumentWriter().markProcessed(md, stage);
        if(!res) {
        	HttpResponseWriter.printNoDocument(response);
        }
        else {
        	HttpResponseWriter.printSaveOk(response, md.getID());
        }
	}
	
	private void handlePendingDocument(HttpRequest request, HttpResponse response) throws IOException {
		logger.trace("handlePendingDocument()");
        HttpEntity requestEntity = ((HttpEntityEnclosingRequest) request).getEntity();
        String requestContent = EntityUtils.toString(requestEntity);
        
        String stage = getParam(request, RemotePipeline.STAGE_PARAM);
        if(stage==null) {
        	HttpResponseWriter.printMissingParameter(response, RemotePipeline.STAGE_PARAM);
        	return;
        }
        
        DatabaseDocument<MongoType> md;
        try {
        	md = nm.getDatabaseConnector().convert(new LocalDocument(requestContent));
        }
        catch(JsonException e) {
        	HttpResponseWriter.printJsonException(response, e);
        	return;
        }
        boolean res = nm.getDatabaseConnector().getDocumentWriter().markPending(md, stage);
        if(!res) {
            logger.debug("Missing document: "+md.toJson());
        	HttpResponseWriter.printNoDocument(response);
        }
        else {
        	HttpResponseWriter.printSaveOk(response, md.getID());
        }
	}
	
	private void handleDiscardedRequest(HttpRequest request, HttpResponse response) throws IOException {
		logger.trace("handleDiscardedRequest()");
        HttpEntity requestEntity = ((HttpEntityEnclosingRequest) request).getEntity();
        String requestContent = EntityUtils.toString(requestEntity);
        
        String stage = getParam(request, RemotePipeline.STAGE_PARAM);
        if(stage==null) {
        	HttpResponseWriter.printMissingParameter(response, RemotePipeline.STAGE_PARAM);
        	return;
        }
        
        DatabaseDocument<MongoType> md;
        try {
        	md = nm.getDatabaseConnector().convert(new LocalDocument(requestContent));
        }
        catch(JsonException e) {
        	HttpResponseWriter.printJsonException(response, e);
        	return;
        }
        
        boolean res = nm.getDatabaseConnector().getDocumentWriter().markDiscarded(md, stage);
        
        if(!res) {
        	HttpResponseWriter.printNoDocument(response);
        }
        else {
        	HttpResponseWriter.printSaveOk(response, md.getID());
        }
	}
 
	private void handleWriteDocument(HttpRequest request, HttpResponse response) throws IOException {
		logger.trace("handleWriteDocument()");
        HttpEntity requestEntity = ((HttpEntityEnclosingRequest) request).getEntity();
        String requestContent = EntityUtils.toString(requestEntity);

        String stage = getParam(request, RemotePipeline.STAGE_PARAM);
        if(stage==null) {
        	HttpResponseWriter.printMissingParameter(response, RemotePipeline.STAGE_PARAM);
        	return;
        }
        
        String partial = getParam(request, RemotePipeline.PARTIAL_PARAM);
        if(partial==null) {
        	HttpResponseWriter.printMissingParameter(response, RemotePipeline.PARTIAL_PARAM);
        	return;
        }
        
        String norelease = getParam(request, RemotePipeline.NORELEASE_PARAM);
        if(norelease==null) {
        	HttpResponseWriter.printMissingParameter(response, RemotePipeline.NORELEASE_PARAM);
        	return;
        }
        
        DatabaseDocument<MongoType> md;
        try {
        	md = nm.getDatabaseConnector().convert(new LocalDocument(requestContent));
        }
        catch(JsonException e) {
        	HttpResponseWriter.printJsonException(response, e);
        	return;
        }
        
        boolean saveRes;
        if(partial.equals("1")) {
        	saveRes = handlePartialWrite(md, response);
        }
        else {
        	if(md.getID()!=null) {
        		saveRes = handleFullUpdate(md, response);
        	}
        	else {
        		saveRes = handleInsert(md, response);
        	}
        }

		if (saveRes && norelease.equals("0")) {
			boolean result = release(md, stage);
			if (!result) {
				HttpResponseWriter.printReleaseFailed(response);
				return;
			}
		}

    }
	
	private boolean handlePartialWrite(DatabaseDocument<MongoType> md, HttpResponse response) throws UnsupportedEncodingException{
		logger.trace("handlePartialWrite()");
		if(md.getID()==null) {
			HttpResponseWriter.printMissingID(response);
			return false;
		}
		logger.debug("Handling a partial write for document "+md.getID());
		DatabaseDocument<MongoType> inDB = nm.getDatabaseConnector().getDocumentReader().getDocumentById(md.getID());
		if(inDB==null) {
			HttpResponseWriter.printUpdateFailed(response, md.getID());
			return false;
		}
		inDB.putAll(md);


		if(nm.getDatabaseConnector().getDocumentWriter().update(inDB)){
			HttpResponseWriter.printSaveOk(response, md.getID());
			return true;
		} 
		else {
			HttpResponseWriter.printUpdateFailed(response, md.getID());
			return false;
		}
	}
	
	private boolean handleFullUpdate(DatabaseDocument<MongoType> md, HttpResponse response) {
		logger.trace("handleFullUpdate()");
		if(nm.getDatabaseConnector().getDocumentWriter().update(md)) {
			HttpResponseWriter.printSaveOk(response, md.getID());
			return true;
		}
		HttpResponseWriter.printUpdateFailed(response, md.getID());
		return false;
	}
	
	private boolean handleInsert(DatabaseDocument<MongoType> md, HttpResponse response) {
		if(nm.getDatabaseConnector().getDocumentWriter().insert(md)) {
			HttpResponseWriter.printInsertOk(response, md);
			return true;
		}
		else {
			HttpResponseWriter.printInsertFailed(response);
			return false;
		}
	}
	
	public static String getParam(HttpRequest request, String param) {
		String uri = getUri(request);
		Pattern p = Pattern.compile("[^\\?]+\\?.*"+param+"=([^&]+)");
		Matcher m = p.matcher(uri);
		if(!m.find()) {
			return null;
		}
		return m.group(1);
	}
	
	public static String getUri(HttpRequest request) {
		return request.getRequestLine().getUri();
	}
	
	private DatabaseQuery<MongoType> requestToQuery(String requestContent) throws JsonException {
		return nm.getDatabaseConnector().convert(new LocalQuery(requestContent));
	}
}