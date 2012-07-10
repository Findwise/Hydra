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

import com.findwise.hydra.NodeMaster;
import com.findwise.hydra.local.RemotePipeline;

public class PropertiesHandler implements ResponsibleHandler {
	Logger logger = LoggerFactory.getLogger(PropertiesHandler.class);
	
	private NodeMaster nm;
	
	public PropertiesHandler(NodeMaster nm) {
		this.nm = nm;
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
        
        if(nm.getPipeline().hasStage(stage)) {
        	map = nm.getPipeline().getStage(stage).getProperties();
        }
        else if(nm.getDatabaseConnector().getPipelineReader().getDebugPipeline().hasStage(stage)){
        	map = nm.getDatabaseConnector().getPipelineReader().getDebugPipeline().getStage(stage).getProperties();
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
