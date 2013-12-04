package com.findwise.hydra.stage;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.http.ParseException;

import com.findwise.hydra.JsonException;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.RemotePipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all stages that do processing of documents in Hydra.
 * 
 * @author anton.hagerstrand
 * @author simon.stenstrom
 * @author anders.rask
 * @author joel.westberg
 * 
 */
public abstract class AbstractProcessStage extends AbstractStage {
    Logger logger = LoggerFactory.getLogger(AbstractProcessStage.class);

	@Parameter(description = "If set, indicates that the document being processed should be FAILED if a ProcessException is thrown by the stage. If not set, the error will only be persisted and the document written back to Hydra.")
	private boolean failDocumentOnProcessException = false;

	@Parameter(description = "The maximum time (in milliseconds) the stage may process a single document before cancelling the processing. Default: -1 (unlimited)")
	private long processingTimeout = -1;
	
	public static final int NUM_RESERVED_ARGUMENTS = 3;
	private long holdInterval = DEFAULT_HOLD_INTERVAL;

	private long terminationTimeout = DEFAULT_SHUTDOWN_TIMEOUT;
	public static final long DEFAULT_SHUTDOWN_TIMEOUT = TimeUnit.SECONDS.toMillis(2);

	// The size of the thread pool is limited to 1, as the pool is only used for timeout functionality, not concurrency
	private final ExecutorService executor;
	private static final int FIXED_THREAD_POOL_SIZE = 1;

	public AbstractProcessStage() {
		this.executor = Executors.newFixedThreadPool(FIXED_THREAD_POOL_SIZE);
	}

	/**
	 * Fetches a document to be processed from the RemotePipeline
	 * 
	 * @return A document to be processed
	 * @throws ParseException
	 * @throws IOException
	 * @throws JsonException
	 */
	protected LocalDocument fetch() throws ParseException, IOException,
			JsonException {
		return getRemotePipeline().getDocument(getQuery());
	}

	/**
	 * Saves the modified document to the RemotePipeline.
	 * 
	 * @throws IOException
	 * @throws JsonException
	 */
	protected boolean persist() throws IOException, JsonException {
		logger.debug("Saving document to RemotePipeline..");
		return getRemotePipeline().saveCurrentDocument();
	}

	/**
	 * Handles errors when processing LocalDocuments. The standard
	 * implementation releases the document back to the RemotePipeline and
	 * writes a log message.
	 * 
	 * @param e
	 *            The thrown error.
	 * @throws IOException
	 * @throws JsonException 
	 */
	protected boolean persistError(LocalDocument d, Exception e) throws IOException, JsonException {
		logger.error("Trying to release document due to error in processing", e);
		d.addError(getStageName(), e);
		return getRemotePipeline().saveCurrentDocument();
	}
	
	/**
	 * Process a LocalDocument, changing that in whatever way a AbstractStage
	 * implementation chooses to.
	 * 
	 * @param doc
	 *            The document to be processed
	 * @throws ProcessException
	 *             Whenever a error in processing occurs
	 */
	public abstract void process(LocalDocument doc) throws ProcessException;

	@Override
	public void setUp(RemotePipeline rp, Map<String, Object> properties) throws IllegalArgumentException, IllegalAccessException, IOException, RequiredArgumentMissingException {
		super.setUp(rp, properties);
	}

	/**
	 * The thread starts with run(). Use run() to start the execution of this
	 * step in this thread, or start() to start it in a new step (recomended).
	 * The execution can be stopped from another thread by calling the
	 * stopStage() method.
	 * 
	 * The run() method operates by trying to fetch documents through fetch(),
	 * processing by process() and then persisting by persist().
	 * 
	 */
	@Override
	public void run() {
		
		setContinueRunning(true);

		while (isContinueRunning()) {
			try {
				LocalDocument doc = fetch();
				if (doc == null) {
					Thread.sleep(holdInterval);
				} else {
					performProcessing(doc);
				}
			} catch (Exception e) {
				logger.error("Caught exception while running", e);
				killStage();
				return;
			}
		}
		shutdownProcessing();
	}

	private void performProcessing(LocalDocument doc) throws Exception {
		logger.debug("Got new doc '{}' to process.", doc.getID());
		Future<Object> future = executor.submit(new ProcessCallable(doc));
		try {
			logger.trace("Waiting for processing of doc '{}'", doc.getID());
			if (processingTimeout > 0) {
				future.get(processingTimeout, TimeUnit.MILLISECONDS);
			} else {
				future.get();
			}
			logger.trace("Processing finished of doc '{}'", doc.getID());
			boolean persistSucceeded = persist();
			if(!persistSucceeded) {
				LocalDocument ld = new LocalDocument(doc.toJson());
				IOException e = new IOException("Unable to save changes to core");
				if(!persistFailure(ld, e)) {
					logger.error("Unable to persist an error to the database for doc '" + doc.getID() + "'", e);
				}
			}
		} catch (ExecutionException e) {
			if (e.getCause() instanceof ProcessException) {
				handleProcessException(doc, (ProcessException)e.getCause());
			} else {
				handleProcessException(doc, e);
				logger.error("Processing for doc '{}' failed execution");
				throw e;
			}
		} catch (TimeoutException e) {
			// Extreme solution here. If the stage thread did not finish in a timely manner,
			// we restart the whole process. Canceling the future doesn't really help, since
			// we can't easily tell whether the thread running the future task manages to shut
			// down cleanly. // TODO: A better solution would be nice.
			handleProcessException(doc, new ProcessException(e));
			throw e;
		}
	}

	private void shutdownProcessing() {
		try {
			executor.shutdown();
			if (!executor.awaitTermination(terminationTimeout, TimeUnit.MILLISECONDS)) {
				logger.error("Processing still in progress, stage is leaving dangling threads");
			}
		} catch (InterruptedException e) {
			logger.error("Interrupted during shutdown");
		}
	}

	private void handleProcessException(LocalDocument doc, Exception e) throws IOException, JsonException {
		if(failDocumentOnProcessException) {
			persistFailure(doc, e);
		} else {
			persistError(doc, e);
		}
	}

	private boolean persistFailure(LocalDocument doc, Exception e) throws IOException {
		logger.debug("Failing doc '{}'", doc.getID());
		return getRemotePipeline().markFailed(doc, e);
	}

	class ProcessCallable implements Callable<Object> {

		private final LocalDocument doc; // TODO: Can stages reassign the doc object?

		ProcessCallable(LocalDocument doc) {
			this.doc = doc;
		}

		@Override
		public Object call() throws ProcessException {
			process(doc);
			return null;
		}
	}
}
