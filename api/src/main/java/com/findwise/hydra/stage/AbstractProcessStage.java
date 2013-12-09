package com.findwise.hydra.stage;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.LocalQuery;

import java.util.concurrent.TimeUnit;

public abstract class AbstractProcessStage {
	@Parameter(description="Number of instances (threads) to start of this stage within a single JVM. Defaults to 1.")
	private int numberOfThreads = 1;

	@Parameter(description="The Query that this stage will receive documents matching")
	private LocalQuery query = new LocalQuery();

	@Parameter(description = "The maximum time (in milliseconds) the stage may process a single document before cancelling the processing. Default: -1 (unlimited)")
	private long processingTimeout = -1;

	@Parameter(description = "If set, indicates that the document being processed should be FAILED if a ProcessException is thrown by the stage. If not set, the error will only be persisted and the document written back to Hydra.")
	private boolean failDocumentOnProcessException = false;

	public abstract void process(LocalDocument document) throws ProcessException;
	public void init() throws RequiredArgumentMissingException, InitFailedException {}

	public int getNumberOfThreads() {
		return numberOfThreads;
	}

	public void setNumberOfThreads(int numberOfThreads) {
		this.numberOfThreads = numberOfThreads;
	}

	public LocalQuery getQuery() {
		return query;
	}

	public void setQuery(LocalQuery query) {
		this.query = query;
	}

	public long getProcessingTimeout() {
		return processingTimeout;
	}

	public void setProcessingTimeout(long processingTimeout) {
		this.processingTimeout = processingTimeout;
	}

	public boolean isFailDocumentOnProcessException() {
		return failDocumentOnProcessException;
	}

	public void setFailDocumentOnProcessException(boolean failDocumentOnProcessException) {
		this.failDocumentOnProcessException = failDocumentOnProcessException;
	}
}
