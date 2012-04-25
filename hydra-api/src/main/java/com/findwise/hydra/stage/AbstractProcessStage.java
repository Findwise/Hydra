package com.findwise.hydra.stage;

import java.io.IOException;
import java.util.Map;

import org.apache.http.HttpException;
import org.apache.http.ParseException;

import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.common.Logger;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.LocalQuery;
import com.findwise.hydra.local.QueryParamTranslator;
import com.findwise.hydra.local.RemotePipeline;
import com.findwise.hydra.local.StaticQueryParamTranslator;

/**
 * 
 * Class to use for creating new process steps in Hydra.
 * 
 * @author anton.hagerstrand
 * @author simon.stenstrom
 * @author anders.rask
 * 
 */
public abstract class AbstractProcessStage extends AbstractStage {

	public static final int NUM_RESERVED_ARGUMENTS = 3;
	private long holdInterval = DEFAULT_HOLD_INTERVAL;
	private static QueryParamTranslator queryParamTranslator = new StaticQueryParamTranslator();
	private LocalQuery localQuery;

	/**
	 * Fetches a document to be processes from the RemotePipeline
	 * 
	 * @return A document to be processed
	 * @throws ParseException
	 * @throws IOException
	 * @throws HttpException
	 * @throws JsonException
	 */
	protected LocalDocument fetch() throws ParseException, IOException,
			HttpException, JsonException {
		return getRemotePipeline().getDocument(getLocalQuery());
	}

	/**
	 * Saves the modified document to the RemotePipeline.
	 * 
	 * @throws IOException
	 * @throws HttpException
	 * @throws JsonException
	 */
	protected void persist() throws IOException, HttpException, JsonException {
		Logger.debug("Saving document to RemotePipeline..");
		getRemotePipeline().saveCurrentDocument();

	}

	/**
	 * Handles errors when processing LocalDocuments. The standard
	 * implementation releases the document back to the RemotePipeline and
	 * writes a log message.
	 * 
	 * @param e
	 *            The thrown error.
	 * @throws IOException
	 * @throws HttpException
	 */
	protected void persistError(ProcessException e) throws IOException,
			HttpException {
		Logger.error("Trying to release document due to error in processing", e);
		getRemotePipeline().releaseLastDocument();
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

	/**
	 * Associate a local query with this stage
	 * 
	 * @param localQuery
	 */
	public void setLocalQuery(LocalQuery localQuery) {
		this.localQuery = localQuery;
	}

	/**
	 * 
	 * @return The localquery associated with this step.
	 */
	public LocalQuery getLocalQuery() {
		return localQuery;
	}

	@Override
	public void setUp(RemotePipeline rp, Map<String, Object> properties) throws IllegalArgumentException, IllegalAccessException, JsonException, IOException, HttpException {
		super.setUp(rp, properties);
		LocalQuery q = queryParamTranslator.createQueryFromList(getQueryOptions());
		setLocalQuery(q);
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
	public void run() {
		
		setContinueRunning(true);

		while (isContinueRunning()) {
			try {
				LocalDocument doc = fetch();
				if (doc == null) {
					Thread.sleep(holdInterval);

				} else {
					try {
						Logger.debug("Got new doc " + doc.getID()
								+ " to process.");
						process(doc);
						persist();
					} catch (ProcessException e) {
						persistError(e);
					}

				}

			} catch (Exception e) {
				Logger.error("Caught exception while running", e);
				Runtime.getRuntime().removeShutdownHook(getShutDownHook());
				System.exit(1);
			}
		}
	}

}
