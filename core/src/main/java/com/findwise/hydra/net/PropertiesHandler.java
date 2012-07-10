package com.findwise.hydra.net;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.local.RemotePipeline;

public class PropertiesHandler<T extends DatabaseType> implements ResponsibleHandler {
	Logger logger = LoggerFactory.getLogger(PropertiesHandler.class);
	
	private DatabaseConnector<T> dbc;
	
	public PropertiesHandler(DatabaseConnector<T> dbc) {
		this.dbc = dbc;
	}
	
	@Override
	public void handle(HttpRequest request, HttpResponse response, HttpContext context)
			throws HttpException, IOException {
		logger.trace("handleGetProperties()");
		
        String stage = RESTTools.getStage(request);
        logger.debug("Received getProperties()-request for stage: "+stage);
        
        if(stage==null) {
        	HttpResponseWriter.printMissingParameter(response, RemotePipeline.STAGE_PARAM);
        	return;
        }
        
        Map<String, Object> map = new HashMap<String, Object>();
        
        if(dbc.getPipelineReader().getPipeline().hasStage(stage)) {
        	map = dbc.getPipelineReader().getPipeline().getStage(stage).getProperties();
        }
        else if(dbc.getPipelineReader().getDebugPipeline().hasStage(stage)){
        	map = dbc.getPipelineReader().getDebugPipeline().getStage(stage).getProperties();
        } 
        
        HttpResponseWriter.printJson(response, map);

	}

	@Override
	public boolean supports(HttpRequest request) {
		return RESTTools.isGet(request) && RESTTools.getBaseUrl(request).equals(RemotePipeline.GET_PROPERTIES_URL);
	}

	@Override
	public String[] getSupportedUrls() {
		return new String[] {RemotePipeline.GET_PROPERTIES_URL};
	}

}
