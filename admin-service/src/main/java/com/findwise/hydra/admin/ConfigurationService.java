package com.findwise.hydra.admin;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.DatabaseFile;
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.Pipeline;
import com.findwise.hydra.Stage;

public class ConfigurationService<T extends DatabaseType> {
	private PipelineScanner<T> pipelineScanner;

	private DatabaseConnector<T> connector; 
	
	private static final String TMP_DIR = "tmp";
	private static Logger logger = LoggerFactory
			.getLogger(ConfigurationService.class);

	public ConfigurationService(DatabaseConnector<T> connector) {
		this.connector = connector;

		try {
			connect();
			pipelineScanner = new PipelineScanner<T>(connector.getPipelineReader());
		} catch (IOException e) {
			logger.error("Failed to connect", e);
		}
	}

	private void connect() throws IOException {
		connector.connect();
	}

	public void addLibrary(String id, InputStream stream) {
		connector.getPipelineWriter().save(id, stream);
	}

	public Map<String, Object> getLibraries() {
		Map<String, Object> map = new HashMap<String, Object>();

		for (DatabaseFile df : pipelineScanner.getLibraryFiles()) {
			map.put(df.getId().toString(), getLibraryMap(df));
		}

		return map;
	}
	
	public Map<String, Object> getLibrary(String id) {
		for (DatabaseFile df : pipelineScanner.getLibraryFiles()) {
			if(df.getId().toString().equals(id)) {
				return getLibraryMap(df);
			}
		}
		return null;
	}

	private Map<String, Object> getLibraryMap(DatabaseFile df) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("filename", df.getFilename());
		map.put("uploaded", df.getUploadDate());
		map.put("stages", getStagesMap(df));
		return map;
	}

	private Map<String, Object> getStagesMap(DatabaseFile df) {
		Map<String, Object> map = new HashMap<String, Object>();
		try {
			for (Class<?> c : pipelineScanner.getStageClasses(new File(TMP_DIR), df)) {
				try {
					map.put(c.getCanonicalName(), new StageInformation(c));
				} catch (NoSuchElementException e) {
					logger.error("Unable to get stage information for class "+c.getCanonicalName(), e);
				}
			}
		} catch (IOException e) {
			logger.error("Unable to get stage classes", e);
		}
		return map;
	}

	public Map<String, Object> getStats() {
		Map<String, Object> map = new HashMap<String, Object>();
		Map<String, Object> documentMap = new HashMap<String, Object>();

		documentMap.put("current", connector.getDocumentReader()
				.getActiveDatabaseSize());
		documentMap.put("throughput", 0);
		documentMap.put("archived", connector.getDocumentReader()
				.getInactiveDatabaseSize());
		documentMap.put("status", new HashMap<String, Long>());

		map.put("documents", documentMap);

		Map<String, Object> stageMap = new HashMap<String, Object>();
		map.put("stages", stageMap);

		stageMap.put("active", getStageConfigMap(connector.getPipelineReader()
				.getPipeline()));

		stageMap.put("debug", getStageConfigMap(connector.getPipelineReader()
				.getDebugPipeline()));

		return map;
	}

	private Map<String, Object> getStageConfigMap(Pipeline<Stage> pipeline) {
		Map<String, Object> map = new HashMap<String, Object>();
		for (Stage s : pipeline.getStages()) {
			HashMap<String, Object> stage = new HashMap<String, Object>();
			stage.put("properties", s.getProperties());

			HashMap<String, Object> file = new HashMap<String, Object>();
			file.put("id", s.getDatabaseFile().getId());
			file.put("name", s.getDatabaseFile().getFilename());
			stage.put("file", file);
			map.put(s.getName(), stage);
		}
		return map;
	}
}
