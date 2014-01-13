package com.findwise.hydra.stage;

import com.findwise.hydra.local.RemotePipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StageFactory {
	private Logger logger = LoggerFactory.getLogger(StageFactory.class);

	private final String stageName;
	private final String hostName;
	private final int port;
	private final boolean usePerformanceLogging;
	private final Map<String, Object> stageOverrideProperties;

	public StageFactory(String stageName, String hostName, int port, boolean usePerformanceLogging, Map<String, Object> stageOverrideProperties) {
		this.stageName = stageName;
		this.hostName = hostName;
		this.port = port;
		this.usePerformanceLogging = usePerformanceLogging;
		this.stageOverrideProperties = stageOverrideProperties;
	}

	public void createAndStartStages() throws UnknownHostException {
		List<AbstractStage> stages = createInstances();

		for(AbstractStage stage : createInstances()) {
			stage.start();
		}
		logger.info("Started "+stages.size()+" instances of stage: " + stages.get(0).getName()+". Running with the query: "+stages.get(0).getQuery());
	}

	private List<AbstractStage> createInstances() throws UnknownHostException {
		List<AbstractStage> stageInstances = new ArrayList<AbstractStage>();

		// Since the number of threads to spawn is an instance variable of the stage, we
		// need to construct at least one in order to know how many to construct. That's
		// why this logic is so strange. TODO: Code smell
		AbstractStage stageInstance = createInstance();
		stageInstances.add(stageInstance);
		while(stageInstances.size() < stageInstance.getNumberOfThreads()) {
			stageInstances.add(createInstance());
		}
		return stageInstances;
	}

	@SuppressWarnings("unchecked")
	private AbstractStage createInstance() throws UnknownHostException {
		try {
			// TODO: Do we really want one per instance? Or can we reuse one for multiple instances?
			RemotePipeline rp = createRemotePipeline();
			Map<String, Object> properties = (stageOverrideProperties != null) ? stageOverrideProperties : rp.getProperties();

			String stageClassName;
			if(properties.containsKey(AbstractStage.ARG_NAME_STAGE_CLASS)) {
				stageClassName = (String) properties.get(AbstractStage.ARG_NAME_STAGE_CLASS);
			} else {
				throw new RequiredArgumentMissingException("No class specified in the '"+ AbstractStage.ARG_NAME_STAGE_CLASS+"' property.");
			}

			AbstractStage stage = (AbstractStage) Class.forName(stageClassName).newInstance();

			// stage.setName sets the name of the *thread*.
			// TODO: Add a numeric identifier to the thread name (for stages with multiple threads).
			stage.setName(stageName);
			stage.setStageName(stageName);

			stage.setUp(rp, properties);
			stage.init();

			return stage;
		} catch (RequiredArgumentMissingException e) {
			throw new StageCreationFailedException("Failed to read arguments", e);
		} catch (InitFailedException e) {
			throw new StageCreationFailedException("Failed to initialize Stage", e);
		} catch (ClassNotFoundException e) {
			throw new StageCreationFailedException("Could not find the Stage class in classpath", e);
		} catch (InstantiationException e) {
			throw new StageCreationFailedException("Could not instantiate the Stage class", e);
		} catch (IllegalAccessException e) {
			throw new StageCreationFailedException("Could not access constructor of Stage class", e);
		} catch (UnknownHostException e) {
			// TODO: Why are we throwing this without wrapping it?
			throw e;
		} catch (IOException e) {
			throw new StageCreationFailedException("Communication failure when reading properties", e);
		}
	}

	private RemotePipeline createRemotePipeline() {
		RemotePipeline rp = new RemotePipeline(hostName, port, stageName);
		rp.setPerformanceLogging(usePerformanceLogging);
		return rp;
	}

	public class StageCreationFailedException extends RuntimeException {
		public StageCreationFailedException(String message, Throwable cause) {
			super(message, cause);
		}
	}
}
