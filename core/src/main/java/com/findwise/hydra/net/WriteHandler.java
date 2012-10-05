package com.findwise.hydra.net;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

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
import com.findwise.hydra.common.Document;
import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.RemotePipeline;
import com.findwise.hydra.net.RESTTools.Method;

public class WriteHandler<T extends DatabaseType> implements ResponsibleHandler {

	private DatabaseConnector<T> dbc;

	private static Logger logger = LoggerFactory.getLogger(WriteHandler.class);

	public WriteHandler(DatabaseConnector<T> dbc) {
		this.dbc = dbc;
	}
	
	@Override
	public void handle(HttpRequest request, HttpResponse response, HttpContext arg2)
			throws HttpException, IOException {
		logger.trace("handleWriteDocument()");
        HttpEntity requestEntity = ((HttpEntityEnclosingRequest) request).getEntity();
        String requestContent = EntityUtils.toString(requestEntity);

        String stage = RESTTools.getParam(request, RemotePipeline.STAGE_PARAM);
        if(stage==null) {
        	HttpResponseWriter.printMissingParameter(response, RemotePipeline.STAGE_PARAM);
        	return;
        }
        
        String partial = RESTTools.getParam(request, RemotePipeline.PARTIAL_PARAM);
        if(partial==null) {
        	HttpResponseWriter.printMissingParameter(response, RemotePipeline.PARTIAL_PARAM);
        	return;
        }
        
        String norelease = RESTTools.getParam(request, RemotePipeline.NORELEASE_PARAM);
        if(norelease==null) {
        	HttpResponseWriter.printMissingParameter(response, RemotePipeline.NORELEASE_PARAM);
        	return;
        }
        
        DatabaseDocument<T> md;
        try {
        	md = dbc.convert(new LocalDocument(requestContent));
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
	
	private boolean release(Document md, String stage) {
		return dbc.getDocumentWriter()
				.markTouched(md.getID(), stage);
	}
	
	private boolean handlePartialWrite(DatabaseDocument<T> md, HttpResponse response) throws UnsupportedEncodingException{
		logger.trace("handlePartialWrite()");
		if(md.getID()==null) {
			HttpResponseWriter.printMissingID(response);
			return false;
		}
		logger.debug("Handling a partial write for document "+md.getID());
		DatabaseDocument<T> inDB = dbc.getDocumentReader().getDocumentById(md.getID());
		if(inDB==null) {
			HttpResponseWriter.printNoDocument(response);
			return false;
		}
		inDB.putAll(md);


		if(dbc.getDocumentWriter().update(inDB)){
			HttpResponseWriter.printSaveOk(response, md.getID());
			return true;
		} 
		else {
			HttpResponseWriter.printSaveFailed(response, md.getID());
			return false;
		}
	}
	
	private boolean handleFullUpdate(DatabaseDocument<T> md, HttpResponse response) {
		logger.trace("handleFullUpdate()");
		if(dbc.getDocumentWriter().update(md)) {
			HttpResponseWriter.printSaveOk(response, md.getID());
			return true;
		}
		HttpResponseWriter.printSaveFailed(response, md.getID());
		return false;
	}
	
	private boolean handleInsert(DatabaseDocument<T> md, HttpResponse response) {
		if(dbc.getDocumentWriter().insert(md)) {
			HttpResponseWriter.printInsertOk(response, md);
			return true;
		}
		else {
			HttpResponseWriter.printInsertFailed(response);
			return false;
		}
	}
	@Override
	public boolean supports(HttpRequest request) {
		return RESTTools.getMethod(request) == Method.POST
				&& RemotePipeline.WRITE_DOCUMENT_URL.equals(RESTTools
						.getBaseUrl(request));
	}

	@Override
	public String[] getSupportedUrls() {
		return new String[] { RemotePipeline.WRITE_DOCUMENT_URL };
	}

}
