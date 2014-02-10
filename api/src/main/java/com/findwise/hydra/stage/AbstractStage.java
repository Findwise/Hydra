package com.findwise.hydra.stage;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.Logging;
import com.findwise.hydra.local.LocalQuery;
import com.findwise.hydra.local.RemotePipeline;

/**
 * 
 * Base class for handling stages in Hydra. Implementations of this
 * class get the RemotePipeline communication as well as Thread handling for
 * free.  
 * 
 * @author anton.hagerstrand
 * @author simon.stenstrom
 * @author anders.rask
 * 
 */
public abstract class AbstractStage extends Thread {
    private static Logger logger = LoggerFactory.getLogger(AbstractStage.class);

	public static final String ARG_NAME_STAGE_CLASS = "stageClass";
	public static final String PROPERTY_NAME_COMMANDLINE_ARGS = "cmdline_args";
	
	@Parameter(description="The Query that this stage will receive documents matching")
	private LocalQuery query = new LocalQuery();
	
	@Parameter(description="Number of instances (threads) to start of this stage within a single JVM. Defaults to 1.")
	private int numberOfThreads = 1;

	private StageKiller stageKiller = new JvmStageKiller();
	public static final int DEFAULT_HOLD_INTERVAL = 2000;
	private RemotePipeline remotePipeline = null;
	private Thread shutDownHook;

	private String stageName;
	private boolean continueRunning;

	/**
	 * Initiates an implementation of AbstractDocument. When this method is
	 * called, and Object of the class has been initialized. The arguments
	 * provided when starting this step is available via getArgument(String
	 * argName)
	 * 
	 * @throws RequiredArgumentMissingException
	 * @throws InitFailedException
	 */
	public void init() throws RequiredArgumentMissingException, InitFailedException {
		
	}

	public int getNumberOfThreads() {
		return numberOfThreads;
	}

	public Thread getShutDownHook() {
		return shutDownHook;
	}

	public void setShutDownHook(Thread shutDownHook) {
		this.shutDownHook = shutDownHook;
	}

	/**
	 * 
	 * @return Returns the RemotePipeline instances used by this Stage
	 */
	public RemotePipeline getRemotePipeline() {
		return remotePipeline;
	}

	/**
	 * 
	 * @param rp
	 *            A RemotePipeline to use
	 */
	public void setRemotePipeline(RemotePipeline rp) {
		this.remotePipeline = rp;
	}

	/**
	 * Method to stop execution of current stage. Calling this method will
	 * result in that no more documents will be processed, after the current
	 * document. Processing of the current document will not be interrupted.
	 */
	public synchronized void stopStage() {
		continueRunning = false;
	}

	protected synchronized boolean isContinueRunning() {
		return continueRunning;
	}

	protected synchronized void setContinueRunning(boolean val) {
		continueRunning = val;
	}

	public void setStageName(String stageName) {
		this.stageName = stageName;
	}

	public String getStageName() {
		return stageName;
	}

	public LocalQuery getQuery() {
		return query;
	}

	public void setStageKiller(StageKiller stageKiller) {
		this.stageKiller = stageKiller;
	}

	public void killStage() {
		this.stageKiller.kill(this);
	}

	public void setUp(RemotePipeline rp, Map<String, Object> properties) throws IllegalArgumentException, IllegalAccessException, IOException, RequiredArgumentMissingException {
		setRemotePipeline(rp);
		AbstractProcessStageMapper.setParameters(this, properties);
		this.createAndApplyShutDownHook();
	}

	public static void main(String args[]) throws Exception {
		try {
			StageCommandLineArguments commandLineArguments = StageCommandLineArguments.parse(args);

			String host = commandLineArguments.getHost();
			String stageName = commandLineArguments.getStageName();
			int port = commandLineArguments.getPort();
			boolean performanceLogging = commandLineArguments.isPerformanceLogging();
			Map<String, Object> stageOverrideProperties = commandLineArguments.getStageOverrideProperties();

			// TODO: Why don't we want to log remotely if args.length == 1 ?
			if( args.length > 1) {
				Logging.setup(host, commandLineArguments.getLogPort());
			}

			new StageFactory(stageName, host, port, performanceLogging, stageOverrideProperties).createAndStartStages();
		} catch(Exception e) {
			logger.error("Caught exception while starting stage(s).", e);
			System.exit(1);
		}
	}

	/**
	 * This method should (not sure if it will work in all environments) run
	 * when the Stage is terminated. If it is run, it is run after letting a
	 * process(LocalDocuement doc) return.
	 * 
	 * The default implementation of this method does nothing, so override it if
	 * you need to do something (for example close file readers etc).
	 */
	public void onDestroy() {
	}

	public Thread createAndApplyShutDownHook() {
		shutDownHook = new OnDestroyThread();
		shutDownHook.setName(stageName);
		Runtime.getRuntime().addShutdownHook(shutDownHook);
		return shutDownHook;
	}
	
	private class OnDestroyThread extends Thread {
		public void run() {
			logger.info("Shutting down stage: " + getName());
			if (AbstractStage.this.isAlive()) {
				AbstractStage.this.setContinueRunning(false);
				try {
					AbstractStage.this.join();
				}
				catch (InterruptedException e) {
				}
			}
			AbstractStage.this.onDestroy();
		}
	}
}
