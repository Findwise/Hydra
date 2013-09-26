package com.findwise.hydra.admin;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import com.findwise.hydra.DatabaseException;
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
	
	private static Logger logger = LoggerFactory
			.getLogger(ConfigurationService.class);

	public ConfigurationService(DatabaseConnector<T> connector) {
		this.connector = connector;

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
			for (DatabaseFile df : getPipelineScanner().getLibraryFiles()) {
				map.put(df.getId().toString(), getLibraryMap(df));
			}
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
		map.put("filename", df.getFilename());
		map.put("uploaded", df.getUploadDate());
		map.put("stages", getPipelineScanner().getStagesMap(df));
		return map;
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
		documentMap.put("current", documentReader.getActiveDatabaseSize());
		documentMap.put("throughput", 0);
		documentMap.put("archived", documentReader.getInactiveDatabaseSize());
		documentMap.put("status", new HashMap<String, Long>());

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
}
