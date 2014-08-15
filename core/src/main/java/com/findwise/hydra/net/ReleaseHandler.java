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
import com.findwise.hydra.DatabaseConnector.ConversionException;
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.Document;
import com.findwise.hydra.JsonException;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.net.RESTTools.Method;

public class ReleaseHandler<T extends DatabaseType> implements ResponsibleHandler {

    private CachingDocumentNIO<T> io;

    private static Logger logger = LoggerFactory
            .getLogger(ReleaseHandler.class);

    public ReleaseHandler(CachingDocumentNIO<T> dbc) {
        this.io = dbc;
    }

    @Override
    public void handle(HttpRequest request, HttpResponse response,
                       HttpContext arg2) throws HttpException, IOException {
        logger.trace("handleReleaseDocument()");
        HttpEntity requestEntity = ((HttpEntityEnclosingRequest) request)
                .getEntity();
        String requestContent = EntityUtils.toString(requestEntity);
        String stage = RESTTools.getParam(request, HttpEndpointConstants.STAGE_PARAM);

        if (stage == null) {
            HttpResponseWriter.printMissingParameter(response,
                    HttpEndpointConstants.STAGE_PARAM);
            return;
        }

        try {
            boolean x = release(io.convert(new LocalDocument(requestContent)), stage);
            if (!x) {
                HttpResponseWriter.printNoDocument(response);
            }
        } catch (JsonException e) {
            HttpResponseWriter.printJsonException(response, e);
            return;
        } catch (ConversionException e) {
            HttpResponseWriter.printUnhandledException(response, e);
            return;
        }

        HttpResponseWriter.printDocumentReleased(response);

    }

    private boolean release(Document<T> md, String stage) {
        return io.markTouched(md.getID(), stage);
    }

    @Override
    public boolean supports(HttpRequest request) {
        return RESTTools.getMethod(request) == Method.POST
                && HttpEndpointConstants.RELEASE_DOCUMENT_URL.equals(RESTTools
                .getBaseUrl(request));
    }

    @Override
    public String[] getSupportedUrls() {
        return new String[] { HttpEndpointConstants.RELEASE_DOCUMENT_URL };
    }

}
