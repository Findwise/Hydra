package com.findwise.hydra.stage;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.JsonException;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.RemotePipeline;

public class ProcessStageRunner {
	Logger logger = LoggerFactory.getLogger(ProcessStageRunner.class);

	private final String stageName;
	private final AbstractProcessStage stage;
	private final RemotePipeline remotePipeline;

	private long terminationTimeout = TimeUnit.SECONDS.toMillis(2);

	public ProcessStageRunner(String stageName, AbstractProcessStage stage, RemotePipeline remotePipeline) {
		this.stageName = stageName;
		this.stage = stage;
		this.remotePipeline = remotePipeline;
	}

	// We use this for timeout functionality, but this class is now called by several threads so we need a thread pool.
	private final ExecutorService executor = Executors.newCachedThreadPool();

	protected void performProcessing(LocalDocument doc) throws Exception {
		logger.debug("Got new doc '{}' to process.", doc.getID());
		try {
			logger.trace("Waiting for processing of doc '{}'", doc.getID());
			processWithTimeout(doc);
			logger.trace("Processing finished of doc '{}'", doc.getID());
		} catch (ExecutionException e) {
			onException(doc, unwrapExecutionException(e));
			return;
		} catch (TimeoutException e) {
			// Extreme solution here. If the stage thread did not finish in a timely manner,
			// we restart the whole process. Canceling the future doesn't really help, since
			// we can't easily tell whether the thread running the future task manages to shut
			// down cleanly. // TODO: A better solution would be nice.
			onException(doc, e);
			throw e;
		} catch (InterruptedException e) {
			logger.info("Processing was interrupted");
			Thread.currentThread().interrupt();
			return;
		}
		onSuccess(doc);
	}

	private Exception unwrapExecutionException(ExecutionException e) {
		Throwable cause = e.getCause();
		if(cause instanceof Error) {
			throw (Error)cause;
		} else {
			return (Exception)cause;
		}
	}

	private void onSuccess(LocalDocument doc) throws IOException, JsonException {
		if(doc.isDiscarded()) {
			remotePipeline.markDiscarded(doc);
		} else {
			// Do not persist if output stage. TODO: Rethink this...
			if(stage instanceof AbstractOutputStage) {
				remotePipeline.markProcessed(doc);
			} else {
				persist(doc);
			}
		}
	}

	private void processWithTimeout(LocalDocument doc) throws InterruptedException, ExecutionException, TimeoutException {
		Future<Object> future = executor.submit(new ProcessCallable(doc, stage));
		if (stage.getProcessingTimeout() > 0) {
			future.get(stage.getProcessingTimeout(), TimeUnit.MILLISECONDS);
		} else {
			future.get();
		}
	}

	private void persist(LocalDocument doc) throws IOException, JsonException {
		logger.debug("Saving document to RemotePipeline..");
		if(!remotePipeline.save(doc)) {
			// TODO: Why are we cloning here?
			LocalDocument ld = new LocalDocument(doc);
			IOException e = new IOException("Unable to save changes to core");
			if(!onException(ld, e)) {
				logger.error("Unable to persist an error to the database for doc '" + doc.getID() + "'", e);
			}
		}
	}

	public void shutdownProcessing() {
		try {
			executor.shutdown();
			if (!executor.awaitTermination(terminationTimeout, TimeUnit.MILLISECONDS)) {
				logger.error("Processing still in progress, stage is leaving dangling threads");
			}
		} catch (InterruptedException e) {
			logger.error("Interrupted during shutdown");
		}
	}

	protected boolean onException(LocalDocument doc, Exception e) throws IOException, JsonException {
		logger.debug("Failing doc '{}'", doc.getID());
		return remotePipeline.markFailed(doc, e);
	}

	static class ProcessCallable implements Callable<Object> {
		private final LocalDocument doc;
		private final AbstractProcessStage processStage;

		ProcessCallable(LocalDocument doc, AbstractProcessStage processStage) {
			this.doc = doc;
			this.processStage = processStage;
		}

		@Override
		public Object call() throws Exception {
			processStage.process(doc);
			return null;
		}
	}
}
