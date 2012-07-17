package com.findwise.hydra.net;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Date;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.common.DocumentFile;
import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.common.SerializationUtils;
import com.findwise.hydra.local.RemotePipeline;

public class FileHandler<T extends DatabaseType> implements ResponsibleHandler {

	private static Logger logger = LoggerFactory.getLogger(FileHandler.class);
	
	private DatabaseConnector<T> dbc;
	
	public FileHandler(DatabaseConnector<T> dbc) {
		this.dbc = dbc;
	}
	
	@Override
	public void handle(HttpRequest request, HttpResponse response, HttpContext context)
			throws HttpException, IOException {
		if(RESTTools.isPost(request)) {
			handleSaveFile(request, response);
		} else if (RESTTools.isGet(request)) {
			if(RESTTools.getParam(request, RemotePipeline.FILENAME_PARAM)!=null) {
				handleGetFile(request, response);
			} else {
				handleGetFilenames(request, response);
			}
		} else if (RESTTools.isDelete(request)) {
			handleDeleteFile(request, response);
		}
	}

	@Override
	public boolean supports(HttpRequest request) {
		return (RESTTools.isGet(request) || RESTTools.isPost(request) || RESTTools.isDelete(request)) && RESTTools.getBaseUrl(request).equals(RemotePipeline.FILE_URL);
	}

	@Override
	public String[] getSupportedUrls() {
		return new String[] { RemotePipeline.FILE_URL };
	}
	
	private void handleSaveFile(HttpRequest request, HttpResponse response) {
		try {
			DocumentFile df = getDocumentFile(request);
			if (df == null) {
				HttpResponseWriter.printBadRequestContent(response);
			}

			DatabaseDocument<T> md = dbc.getDocumentReader().getDocumentById(df.getDocumentId());
			if (md == null) {
				HttpResponseWriter.printNoDocument(response);
				return;
			}
			
			dbc.getDocumentWriter().write(df);
			
			HttpResponseWriter.printOk(response);
		} catch (Exception e) {
			logger.error("An error occurred during file save", e);
			HttpResponseWriter.printUnhandledException(response, e);
		}
	}
	
	private void handleGetFile(HttpRequest request, HttpResponse response) throws IOException {
        Triple triple = getTriple(request, response);
        if(triple==null) {
        	return;
        }
        
        DatabaseDocument<T> md = dbc.getDocumentReader().getDocumentById(triple.docid);
        if(md==null) {
        	HttpResponseWriter.printNoDocument(response);
        	return;
        }
        
        DocumentFile df = dbc.getDocumentReader().getDocumentFile(md, triple.fileName);
        
        if(df==null) {
        	HttpResponseWriter.printFileNotFound(response, triple.fileName);
        	return;
        }
        
        HttpResponseWriter.printJson(response, df);
	}
	
	private void handleGetFilenames(HttpRequest request, HttpResponse response) throws IOException {
		Tuple tuple = getTuple(request, response);
		
		if(tuple == null) {
			return;
		}
		
        DatabaseDocument<T> md = dbc.getDocumentReader().getDocumentById(tuple.docid);
        if(md==null) {
        	HttpResponseWriter.printNoDocument(response);
        	return;
        }
		
        HttpResponseWriter.printJson(response, dbc.getDocumentReader().getDocumentFileNames(md));
	}
	
	@SuppressWarnings("unchecked")
	private DocumentFile getDocumentFile(HttpRequest request) throws ParseException, IOException, JsonException {
        HttpEntity requestEntity = ((HttpEntityEnclosingRequest) request).getEntity();
        String requestContent = EntityUtils.toString(requestEntity, "UTF-8");

		Object o = SerializationUtils.fromJson(requestContent);
		
		if(!(o instanceof Map)) {
			return null;
		}
		
		Map<String, Object> map = (Map<String, Object>) o;
		Object id = dbc.getDocumentReader().toDocumentId(map.get("documentId"));
		String fileName = (String) map.get("fileName");
		Date d = (Date) map.get("uploadDate");
		String encoding = (String) map.get("encoding");
		String mimetype = (String) map.get("mimetype");
		String savedByStage = (String) map.get("savedByStage");
		InputStream is;
		if(encoding == null) {
			is = new ByteArrayInputStream(Base64.decodeBase64(((String)map.get("stream")).getBytes("UTF-8")));
		} else {
			is = new ByteArrayInputStream(Base64.decodeBase64(((String)map.get("stream")).getBytes(encoding)));
		}
		
		DocumentFile df = new DocumentFile(id, fileName, is, savedByStage, d);
		df.setEncoding(encoding);
		df.setMimetype(mimetype);
		
		return df;
	}
	
	private void handleDeleteFile(HttpRequest request, HttpResponse response) throws IOException {
		Triple triple = getTriple(request, response);
		
		if(triple == null) {
			return;
		}
		
        DatabaseDocument<T> md = dbc.getDocumentReader().getDocumentById(triple.docid);
        if(md==null) {
        	HttpResponseWriter.printNoDocument(response);
        	return;
        }
        
       if(dbc.getDocumentWriter().deleteDocumentFile(md, triple.fileName)) {
    	   HttpResponseWriter.printFileDeleteOk(response, triple.fileName, md.getID());
    	   return;
       } 
       HttpResponseWriter.printFileNotFound(response, triple.fileName);
	}
	
	private Tuple getTuple(HttpRequest request, HttpResponse response) {
		Tuple tuple = new Tuple();
		tuple.stage = RESTTools.getParam(request, RemotePipeline.STAGE_PARAM);
        if(tuple.stage==null) {
        	HttpResponseWriter.printMissingParameter(response, RemotePipeline.STAGE_PARAM);
        	return null;
        }

		String rawparam = RESTTools.getParam(request, RemotePipeline.DOCID_PARAM);
		if(rawparam==null) {
        	HttpResponseWriter.printMissingParameter(response, RemotePipeline.DOCID_PARAM);
        	return null;
        }
		try {
			tuple.docid = dbc.getDocumentReader().toDocumentIdFromJson(URLDecoder.decode(rawparam, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			
		}
		if(tuple.docid==null) {
        	HttpResponseWriter.printUnhandledException(response, new SerializationException("Unable to deserialize the parameter "+RemotePipeline.DOCID_PARAM));
        	return null;
        }
        
        return tuple;
	}
	
	private Triple getTriple(HttpRequest request, HttpResponse response) {
		Triple triple = new Triple();

		Tuple tuple = getTuple(request, response);
		if(tuple==null) {
			return null;
		}
		
		triple.docid = tuple.docid;
		triple.stage = tuple.stage;
        
        triple.fileName = RESTTools.getParam(request, RemotePipeline.FILENAME_PARAM);
        if(triple.fileName==null) {
        	HttpResponseWriter.printMissingParameter(response, RemotePipeline.FILENAME_PARAM);
        	return null;
        }
        
        return triple;
	}
	
	private class Tuple {
		String stage;
		Object docid;
	}
	
	private class Triple {
		String stage;
		Object docid;
		String fileName;
	}
}
