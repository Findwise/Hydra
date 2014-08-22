package com.findwise.hydra.admin;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.findwise.hydra.DatabaseException;
import com.findwise.hydra.PipelineStatus;
import com.findwise.hydra.Stage;
import com.findwise.hydra.admin.rest.StageClassNotFoundException;
import com.findwise.hydra.stage.AbstractStage;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.DatabaseFile;
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.DocumentReader;
import com.findwise.hydra.PipelineReader;

public class ConfigurationService<T extends DatabaseType> {
	private PipelineScanner<T> pipelineScanner;

	private DatabaseConnector<T> connector;

	private final Cache<String, Map<String, StageInformation>> libraryCache;
	
	private static Logger logger = LoggerFactory
			.getLogger(ConfigurationService.class);

	public ConfigurationService(DatabaseConnector<T> connector) {
		this.connector = connector;

		this.libraryCache = CacheBuilder.newBuilder().build();
		try {
			initialize();
		} catch (IOException e) {
			logger.error("Failed to connect", e);
		}
	}

	private void initialize() throws IOException {
		connect();
		pipelineScanner = new PipelineScanner<T>(connector.getPipelineReader());
	}

	private void connect() throws IOException {
		connector.connect();
	}

	/**
	 *
	 * @param id the id that the library will be stored as
	 * @param filename the library file name
	 * @param stream library file stream
	 * @throws DatabaseException if connecting to the database failed
	 */
	public void addLibrary(String id, String filename, InputStream stream) throws DatabaseException {
		try {
			getConnector().getPipelineWriter().save(id, filename, stream);
			libraryCache.invalidate(id);
		} catch (IOException e) {
			throw new DatabaseException("Failed to connect to database", e);
		}
	}

	/**
	 *
	 * @return map of available libraries
	 * @throws DatabaseException if scanning the pipeline failed
	 */
	public Map<String, Object> getLibraries() throws DatabaseException {
		try {
			Map<String, Object> map = new HashMap<String, Object>();
			List<Map<String, Object>> libraries = new ArrayList<Map<String, Object>>();

			for (DatabaseFile df : getPipelineScanner().getLibraryFiles()) {
				libraries.add(getLibraryMap(df));
			}
			map.put("libraries", libraries);
			return map;
		} catch (IOException e) {
			throw new DatabaseException("Failed to scan pipeline", e);
		}
	}

	/**
	 *
	 * @param id the id of the library
	 * @return map describing the library, or null if the library id does not exist
	 * @throws DatabaseException if scanning the pipeline failed
	 */
	public Map<String, Object> getLibrary(String id) throws DatabaseException {
		try {
			for (DatabaseFile df : getPipelineScanner().getLibraryFiles()) {
				if(df.getId().toString().equals(id)) {
					return getLibraryMap(df);
				}
			}
			return null;
		} catch (IOException e) {
			throw new DatabaseException("Failed to scan pipeline", e);
		}
	}

	private Map<String, Object> getLibraryMap(DatabaseFile df) throws IOException {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("id", df.getId());
		map.put("filename", df.getFilename());
		map.put("uploaded", df.getUploadDate());
		map.put("stages", getStagesMap(df));
		return map;
	}

	private Map<String, StageInformation> getStagesMap(DatabaseFile df) throws IOException {
		Map<String, StageInformation> stages;
		try {
			stages = libraryCache.get(df.getId().toString(), new LibraryCallable(df));
		} catch (ExecutionException e) {
			throw new IOException("Failed to fetch stages from library", e);
		}
		return stages;
	}

	/**
	 * Modifies the supplied stage configuration with parameters from the stage class
	 *
	 * @param stage a stage configuration
	 */
	@SuppressWarnings("unchecked")
	public void addStageParameters(Stage stage) throws DatabaseException, StageClassNotFoundException {
		try {
			Map<String, StageInformation> stages = getStagesMap(stage.getDatabaseFile());
			Map<String, Object> properties = stage.getProperties();
			String stageClass = (String) properties.get(AbstractStage.ARG_NAME_STAGE_CLASS);
			if (null != stageClass && stages.containsKey(stageClass)) {
				Map<String, Object> parameters = (Map<String, Object>) stages.get(stageClass).get("parameters");
				for (String parameterName : parameters.keySet()) {
					Map<String, Object> parameter = (Map<String, Object>) parameters.get(parameterName);
					if (properties.containsKey(parameterName)) {
						parameter.put("value", properties.get(parameterName));
					}
					properties.put(parameterName, parameter);
				}
				properties.put("stageName", stage.getName());
				properties.put("stageGroup", connector.getPipelineReader().getPipeline().getGroupForStage(stage.getName()).getName());
				properties.put("libId", stage.getDatabaseFile().getId());
			} else {
				throw new StageClassNotFoundException("Stage class '" + stageClass
						+ "' for stage '" + stage.getName() + "' not found."
						+ " Available stage classes: '" + stages.keySet() + "'");
			}
		} catch (IOException e) {
			throw new DatabaseException("Failed to scan pipeline", e);
		}
	}

	/**
	 *
	 * @return map of statistics and current stage groups
	 * @throws DatabaseException if connecting to the database failed
	 */
	public Map<String, Object> getStats() throws DatabaseException {
		Map<String, Object> map = new HashMap<String, Object>();
		Map<String, Object> documentMap = new HashMap<String, Object>();
		DatabaseConnector<T> databaseConnector = null;
		PipelineScanner<T> scanner = null;
		try {
			databaseConnector = getConnector();
			scanner = getPipelineScanner();
		} catch (IOException e) {
			throw new DatabaseException("Failed to connect to database", e);
		}
		DocumentReader<T> documentReader = databaseConnector.getDocumentReader();
		PipelineStatus<T> pipelineStatus = databaseConnector.getStatusReader().getStatus();
		documentMap.put("current", documentReader.getActiveDatabaseSize());
		documentMap.put("throughput", 0); // TODO actually implement this
		documentMap.put("archived", documentReader.getInactiveDatabaseSize());
		documentMap.put("processed", pipelineStatus.getProcessedCount());
		documentMap.put("discarded", pipelineStatus.getDiscardedCount());
		documentMap.put("failed", pipelineStatus.getFailedCount());

		map.put("documents", documentMap);

		Map<String, Object> stageMap = new HashMap<String, Object>();
		map.put("groups", stageMap);

		PipelineReader pipelineReader = databaseConnector.getPipelineReader();
		stageMap.put("active", scanner.getStageConfigMap(pipelineReader.getPipeline()));

		stageMap.put("debug", scanner.getStageConfigMap(pipelineReader.getDebugPipeline()));

		return map;
	}

	private DatabaseConnector<T> getConnector() throws IOException {
		if (!connector.isConnected()) {
			initialize();
		}
		return connector;
	}

	private PipelineScanner<T> getPipelineScanner() throws IOException {
		if (!connector.isConnected() && null == pipelineScanner) {
			initialize();
		}
		return pipelineScanner;
	}

	private class LibraryCallable implements Callable<Map<String, StageInformation>> {

		private final DatabaseFile databaseFile;

		private LibraryCallable(DatabaseFile databaseFile) {
			this.databaseFile = databaseFile;
		}

		@Override
		public Map<String, StageInformation> call() throws Exception {
			return getPipelineScanner().getStagesMap(databaseFile);
		}
	}
}
