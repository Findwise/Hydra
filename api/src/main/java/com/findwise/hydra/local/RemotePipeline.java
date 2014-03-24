package com.findwise.hydra.local;

import com.findwise.hydra.DocumentFileRepository;
import com.findwise.hydra.JsonException;
import com.findwise.hydra.stage.AbstractProcessStage;
import com.findwise.hydra.stage.InitFailedException;
import com.findwise.hydra.stage.RequiredArgumentMissingException;

import java.io.IOException;

public interface RemotePipeline extends DocumentFileRepository {

    int DEFAULT_LOG_PORT = 12002;

    /**
     * Non-recurring, use this in all known cases except for in an output node.
     * <p/>
     * The fetched document will be tagged with the name of the stage which is
     * used to execute getDocument.
     */
    LocalDocument getDocument(LocalQuery query) throws IOException;

    /**
     * Writes an entire document to the pipeline. Use is discouraged, try using save(..) whenever possible.
     */
    boolean saveFull(LocalDocument d) throws IOException, JsonException;

    /**
     * Writes all outstanding updates to the document since it was initialized.
     */
    boolean save(LocalDocument d) throws IOException, JsonException;

    boolean markPending(LocalDocument d) throws IOException;

    boolean markFailed(LocalDocument d) throws IOException;

    boolean markFailed(LocalDocument d, Throwable t) throws IOException;

    boolean markProcessed(LocalDocument d) throws IOException;

    boolean markDiscarded(LocalDocument d) throws IOException;

    AbstractProcessStage getStageInstance() throws IOException, IllegalAccessException, InitFailedException, InstantiationException, JsonException, RequiredArgumentMissingException, ClassNotFoundException;

    String getStageName();

    boolean isPerformanceLogging();
}
