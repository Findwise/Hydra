package com.findwise.hydra.net;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

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
import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.Document;
import com.findwise.hydra.JsonException;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.net.RESTTools.Method;

public class WriteHandler<T extends DatabaseType> implements ResponsibleHandler {

    private CachingDocumentNIO<T> io;
    private boolean performanceLogging;

    private static Logger logger = LoggerFactory.getLogger(WriteHandler.class);

    public WriteHandler(CachingDocumentNIO<T> dbc, boolean performanceLogging) {
        this.io = dbc;
        this.performanceLogging = performanceLogging;
    }

    @Override
    public void handle(HttpRequest request, HttpResponse response, HttpContext arg2)
            throws HttpException, IOException {
        logger.trace("handleWriteDocument()");
        long start = System.currentTimeMillis();
        HttpEntity requestEntity = ((HttpEntityEnclosingRequest) request).getEntity();
        String requestContent = EntityUtils.toString(requestEntity);
        long tostring = System.currentTimeMillis();

        String stage = RESTTools.getParam(request, HttpEndpointConstants.STAGE_PARAM);
        if(stage==null) {
            HttpResponseWriter.printMissingParameter(response, HttpEndpointConstants.STAGE_PARAM);
            return;
        }

        String partial = RESTTools.getParam(request, HttpEndpointConstants.PARTIAL_PARAM);
        if(partial==null) {
            HttpResponseWriter.printMissingParameter(response, HttpEndpointConstants.PARTIAL_PARAM);
            return;
        }

        String norelease = RESTTools.getParam(request, HttpEndpointConstants.NORELEASE_PARAM);
        if(norelease==null) {
            HttpResponseWriter.printMissingParameter(response, HttpEndpointConstants.NORELEASE_PARAM);
            return;
        }

        DatabaseDocument<T> md;
        try {
            md = io.convert(new LocalDocument(requestContent));
        }
        catch(JsonException e) {
            HttpResponseWriter.printJsonException(response, e);
            return;
        } catch (ConversionException e) {
            logger.error("Caught Exception when trying to convert "+requestContent, e);
            HttpResponseWriter.printBadRequestContent(response);
            return;
        }

        long convert = System.currentTimeMillis();

        String type;
        boolean saveRes;
        if(partial.equals("1")) {
            saveRes = handlePartialWrite(md, response);
            type="update";
        }
        else {
            if(md.getID()!=null) {
                saveRes = handleFullUpdate(md, response);
            }
            else {
                saveRes = handleInsert(md, response);
            }
            type="insert";
        }
        long write = System.currentTimeMillis();

        if (saveRes && norelease.equals("0")) {
            boolean result = release(md, stage);
            if (!result) {
                HttpResponseWriter.printReleaseFailed(response);
                return;
            }
        }
        if(performanceLogging) {
            long end = System.currentTimeMillis();
            logger.info(String.format("type=performance event=%s stage_name=%s doc_id=\"%s\" start=%d end=%d total=%d entitystring=%d parse=%d query=%d serialize=%d", type, stage, md.getID(), start, end, end-start, tostring-start, convert-tostring, write-convert, end-write));
        }
    }

    private boolean release(Document<T> md, String stage) {
        return io.markTouched(md.getID(), stage);
    }

    private boolean handlePartialWrite(DatabaseDocument<T> md, HttpResponse response) throws UnsupportedEncodingException{
        logger.trace("handlePartialWrite()");
        if(md.getID()==null) {
            HttpResponseWriter.printMissingID(response);
            return false;
        }
        logger.debug("Handling a partial write for document "+md.getID());
        DatabaseDocument<T> inDB = io.getDocumentById(md.getID());
        if(inDB==null) {
            HttpResponseWriter.printNoDocument(response);
            return false;
        }
        inDB.putAll(md);


        if(io.update(inDB)){
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
        if(io.update(md)) {
            HttpResponseWriter.printSaveOk(response, md.getID());
            return true;
        }
        HttpResponseWriter.printSaveFailed(response, md.getID());
        return false;
    }

    private boolean handleInsert(DatabaseDocument<T> md, HttpResponse response) {
        if(io.insert(md)) {
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
                && HttpEndpointConstants.WRITE_DOCUMENT_URL.equals(RESTTools
                .getBaseUrl(request));
    }

    @Override
    public String[] getSupportedUrls() {
        return new String[] { HttpEndpointConstants.WRITE_DOCUMENT_URL };
    }

}
