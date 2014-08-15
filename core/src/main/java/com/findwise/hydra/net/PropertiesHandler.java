package com.findwise.hydra.net;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.findwise.hydra.local.HttpEndpointConstants;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.Pipeline;
import com.findwise.hydra.PipelineReader;
import com.findwise.hydra.stage.GroupStarter;

public class PropertiesHandler<T extends DatabaseType> implements ResponsibleHandler {
    Logger logger = LoggerFactory.getLogger(PropertiesHandler.class);

    private PipelineReader reader;

    public PropertiesHandler(PipelineReader reader) {
        this.reader = reader;
    }

    @Override
    public void handle(HttpRequest request, HttpResponse response, HttpContext context)
            throws HttpException, IOException {
        if(RESTTools.getBaseUrl(request).equals(HttpEndpointConstants.GET_PROPERTIES_URL)) {
            getPropetries(request, response, context);
        } else if(RESTTools.getBaseUrl(request).equals(GroupStarter.GET_STAGES_URL)) {
            getStages(request, response, context);
        } else {
            logger.error("Unsupported request to PropertiesHandler. Request URL was: "+RESTTools.getUri(request));
        }
    }

    private void getStages(HttpRequest request, HttpResponse response,
                           HttpContext context) {
        logger.trace("handleGetStages()");
        String group = RESTTools.getParam(request, GroupStarter.GROUP_PARAM);

        Pipeline p = reader.getPipeline();
        if(!p.hasGroup(group)) {
            p = reader.getDebugPipeline();
        }
        if(p.hasGroup(group)) {
            HttpResponseWriter.printJson(response, p.getGroup(group).getStageNames());
            return;
        }

        response.setStatusCode(HttpStatus.SC_NOT_FOUND);
        response.setEntity(new NStringEntity("", HttpResponseWriter.CONTENT_TYPE));
    }

    private void getPropetries(HttpRequest request, HttpResponse response,
                               HttpContext context) {
        logger.trace("handleGetProperties()");

        String stage = RESTTools.getStage(request);
        logger.debug("Received getProperties()-request for stage: "+stage);

        if(stage==null) {
            HttpResponseWriter.printMissingParameter(response, HttpEndpointConstants.STAGE_PARAM);
            return;
        }

        Map<String, Object> map = new HashMap<String, Object>();

        if(reader.getPipeline().hasStage(stage)) {
            map = reader.getPipeline().getStage(stage).getProperties();
        }
        else if(reader.getDebugPipeline().hasStage(stage)){
            map = reader.getDebugPipeline().getStage(stage).getProperties();
        }

        HttpResponseWriter.printJson(response, map);
    }

    @Override
    public boolean supports(HttpRequest request) {
        return RESTTools.isGet(request) && (RESTTools.getBaseUrl(request).equals(HttpEndpointConstants.GET_PROPERTIES_URL) || RESTTools.getBaseUrl(request).equals(GroupStarter.GET_STAGES_URL));
    }

    @Override
    public String[] getSupportedUrls() {
        return new String[] {HttpEndpointConstants.GET_PROPERTIES_URL, GroupStarter.GET_STAGES_URL};
    }

}
