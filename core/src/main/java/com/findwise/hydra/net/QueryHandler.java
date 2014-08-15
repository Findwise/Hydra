package com.findwise.hydra.net;

import java.io.IOException;

import com.findwise.hydra.local.HttpEndpointConstants;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.CachingDocumentNIO;
import com.findwise.hydra.DatabaseQuery;
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.Document;
import com.findwise.hydra.JsonException;
import com.findwise.hydra.StageManager;
import com.findwise.hydra.local.LocalQuery;
import com.findwise.hydra.net.RESTTools.Method;

public class QueryHandler<T extends DatabaseType> implements ResponsibleHandler {

    private CachingDocumentNIO<T> io;
    private boolean performanceLogging = false;

    private static Logger logger = LoggerFactory.getLogger(QueryHandler.class);

    public QueryHandler(CachingDocumentNIO<T> dbc, boolean performanceLogging) {
        this.io = dbc;
        this.performanceLogging = performanceLogging;
    }

    @Override
    public void handle(HttpRequest request, HttpResponse response,
                       HttpContext arg2) throws HttpException, IOException {
        long start = System.currentTimeMillis();
        logger.trace("handleGetDocument()");
        HttpEntity requestEntity = ((HttpEntityEnclosingRequest) request).getEntity();
        String requestContent = EntityUtils.toString(requestEntity);
        long tostring = System.currentTimeMillis();
        String stage = RESTTools.getParam(request, HttpEndpointConstants.STAGE_PARAM);

        if (stage == null) {
            HttpResponseWriter.printMissingParameter(response,
                    HttpEndpointConstants.STAGE_PARAM);
            return;
        }

        DatabaseQuery<T> dbq;
        try {
            dbq = requestToQuery(requestContent);
        } catch (JsonException e) {
            HttpResponseWriter.printJsonException(response, e);
            return;
        }

        long parse = System.currentTimeMillis();

        reportQuery(stage);


        Document<T> d = io.getAndTag(dbq, stage);

        long query = System.currentTimeMillis();

        if (d != null) {
            HttpResponseWriter.printDocument(response, d, stage);
        } else {
            HttpResponseWriter.printNoDocument(response);
        }

        if(performanceLogging) {
            long serialize = System.currentTimeMillis();
            Object id = d != null ? d.getID() : null;
            logger.info(String.format("type=performance event=query stage_name=%s doc_id=\"%s\" start=%d end=%d total=%d entitystring=%d parse=%d query=%d serialize=%d", stage, id, start, serialize, serialize-start, tostring-start, parse-tostring, query-parse, serialize-query));
        }
    }

    private DatabaseQuery<T> requestToQuery(String requestContent)
            throws JsonException {
        return io.convert(new LocalQuery(requestContent));
    }

    @Override
    public boolean supports(HttpRequest request) {
        return RESTTools.getMethod(request) == Method.POST
                && HttpEndpointConstants.GET_DOCUMENT_URL.equals(RESTTools
                .getBaseUrl(request));
    }

    @Override
    public String[] getSupportedUrls() {
        return new String[] { HttpEndpointConstants.GET_DOCUMENT_URL };
    }

    private void reportQuery(String stage) {
        StageManager sm = StageManager.getStageManager();

        if(sm.hasRunnerForStage(stage)) {
            sm.getRunnerForStage(stage).setHasQueried();
        }
    }

}
