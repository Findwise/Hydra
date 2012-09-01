package com.findwise.hydra;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.ProcessDestroyer;
import org.apache.commons.exec.PumpStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StageRunner extends Thread {
	private StoredStage stage;
	private Logger logger = LoggerFactory.getLogger(getClass());
	private StageDestroyer stageDestroyer;
	private int timesToRetry;
	private int timesStarted;
	private boolean loggingEnabled;
	private String jvmParameters;
	private String startupArgsString;
	
	private boolean hasQueried = false;
	
	private int pipelinePort;
	
	public synchronized void setHasQueried() {
		hasQueried = true;
	}
	
	public synchronized boolean hasQueried() {
		return hasQueried;
	}

	/**
	 * Expected exit value should be 143 on *NIX systems, the exit value on a
	 * TERM command
	 */
	/** Checked but not conclusively verified to be 143 on Windows as well. */
	public static final int TERM_EXITCODE = 143;

	public StageRunner(StoredStage stage, int pipelinePort) {
		this.stage = stage;
		this.pipelinePort = pipelinePort;
		timesStarted = 0;
		setParameters();
	}

	public final void setParameters() {
		Map<String, Object> conf = stage.getProperties();
		if (conf.containsKey("jvm_parameters")) {
			jvmParameters = (String) conf.get("jvm_parameters");
		}
		else {
			jvmParameters = null;
		}
		if (conf.containsKey("retries")) {
			timesToRetry = (Integer) conf.get("retries");
		}
		else {
			timesToRetry = -1;
		}
		if (conf.containsKey("logging_enabled")) {
			loggingEnabled = (Boolean) conf.get("logging_enabled");
		}
		else {
			loggingEnabled = true;
		}
		if (conf.containsKey("cmdline_args")) {
			startupArgsString = (String) conf.get("cmdline_args");
		}
		else {
			startupArgsString = null;
		}
	}

	public void run() {

		do {
			logger.info("Starting stage " + stage.getName()
					+ ". Times started so far: " + timesStarted);
			timesStarted++;
			boolean cleanShutdown = startStage();
			if (cleanShutdown) {
				return;
			} 
			if(!hasQueried()) {
				logger.error("The stage "+stage.getName()+" did not start. It will not be restarted until configuration changes.");
				return;
			}
		} while (timesToRetry == -1 || timesToRetry >= timesStarted);

		logger.error("Stage " + stage.getName()
				+ " has failed and cannot be restarted. ");
	}
	
	public void printJavaVersion() {
		CommandLine cmdLine = new CommandLine("java");
		cmdLine.addArgument("-version");
		DefaultExecuteResultHandler resultHandler = new DefaultExecuteResultHandler();
		Executor executor = new DefaultExecutor();
		try {
			executor.execute(cmdLine, resultHandler);
		} catch (ExecuteException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private boolean startStage() {
		CommandLine cmdLine = new CommandLine("java");
		cmdLine.addArgument(jvmParameters, false);
		cmdLine.addArgument("-jar");
		cmdLine.addArgument("${file}");
		cmdLine.addArgument(stage.getName());
		cmdLine.addArgument("localhost");
		cmdLine.addArgument(""+pipelinePort);
		cmdLine.addArgument(startupArgsString);
		HashMap<String, File> map = new HashMap<String, File>();
		map.put("file", stage.getFile().getAbsoluteFile());
		cmdLine.setSubstitutionMap(map);
		logger.debug("Launching with command " + cmdLine.toString());
		DefaultExecuteResultHandler resultHandler = new DefaultExecuteResultHandler();

		Executor executor = new DefaultExecutor();

		/** Ensure this process is stopped if the main JVM dies */
		stageDestroyer = new StageDestroyer();
		executor.setProcessDestroyer(stageDestroyer);
		executor.setExitValue(TERM_EXITCODE);

		if (loggingEnabled) {
			// Set up our generic logging service
			PumpStreamHandler streamHandler = new PumpStreamHandler(
					new StreamLogger(stage.getName()));
			executor.setStreamHandler(streamHandler);
		}

		try {
			executor.execute(cmdLine, resultHandler);
		} catch (ExecuteException e) {
			logger.error("Unable to execute " + cmdLine.toString(), e);
		} catch (IOException e) {
			logger.error("Unable to execute " + cmdLine.toString(), e);
		}

		/** Let's wait and see if the process exits */

		try {
			resultHandler.waitFor();
		} catch (InterruptedException e) {
			logger.error("resultHandler was interrupted", e);
		}

		int exitValue = resultHandler.getExitValue();
		if (executor.isFailure(exitValue)) {
			logger.error("Stage " + stage.getName()
					+ " terminated unexpectedly with exit value " + exitValue);
			/** The stage crashed. Restart if we have restarts left */
			return false;
		}
		return true;
	}

	/**
	 * Destroys the JVM running this stage. Should a JVM shutdown fail, it will
	 * throw an IllegalStateException.
	 */
	public void destroy() {
		logger.debug("Attempting to destroy JVM running stage "
				+ stage.getName());
		boolean success = stageDestroyer.killAll();
		if (success) {
			logger.debug("... destruction successful");
		} else {
			logger.error("JVM running stage " + stage.getName()
					+ " was not killed");
			throw new IllegalStateException("Orphaned process for "
					+ stage.getName());
		}
	}

	/**
	 * Manages the destruction of any Stages launched in this wrapper.
	 * Automatically binds to the Runtime to shut down along with the master
	 * JVM.
	 * 
	 * @author joel.westberg
	 * 
	 */
	static class StageDestroyer extends Thread implements ProcessDestroyer {
		private final List<Process> processes = new ArrayList<Process>();

		@Override
		public boolean add(Process p) {
			/**
			 * Register this destroyer to Runtime in order to avoid orphaned
			 * processes if the main JVM dies
			 */
			Runtime.getRuntime().addShutdownHook(this);
			processes.add(p);
			return true;
		}

		@Override
		public boolean remove(Process p) {
			return processes.remove(p);
		}

		@Override
		public int size() {
			return processes.size();
		}

		/**
		 * Invoked by the runtime ShutdownHook on JVM exit
		 */
		public void run() {
			killAll();
		}

		public boolean killAll() {
			boolean success = true;
			synchronized (processes) {
				for (Process process : processes) {
					try {
						process.destroy();
					} catch (RuntimeException t) {
						success = false;
					}
				}
			}
			return success;
		}

	}
}
