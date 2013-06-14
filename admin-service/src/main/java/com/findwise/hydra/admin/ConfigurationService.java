package com.findwise.hydra.admin;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

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

	public void addLibrary(String id, String filename, InputStream stream) throws IOException {
		getConnector().getPipelineWriter().save(id, filename, stream);
	}

	public Map<String, Object> getLibraries() throws IOException {
		Map<String, Object> map = new HashMap<String, Object>();

		for (DatabaseFile df : getPipelineScanner().getLibraryFiles()) {
			map.put(df.getId().toString(), getLibraryMap(df));
		}

		return map;
	}
	
	public Map<String, Object> getLibrary(String id) throws IOException {
		for (DatabaseFile df : getPipelineScanner().getLibraryFiles()) {
			if(df.getId().toString().equals(id)) {
				return getLibraryMap(df);
			}
		}
		return null;
	}

	private Map<String, Object> getLibraryMap(DatabaseFile df) throws IOException {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("filename", df.getFilename());
		map.put("uploaded", df.getUploadDate());
		map.put("stages", getPipelineScanner().getStagesMap(df));
		return map;
	}


	public Map<String, Object> getStats() throws IOException {
		Map<String, Object> map = new HashMap<String, Object>();
		Map<String, Object> documentMap = new HashMap<String, Object>();
		DocumentReader<T> documentReader = getConnector().getDocumentReader();
		documentMap.put("current", documentReader.getActiveDatabaseSize());
		documentMap.put("throughput", 0);
		documentMap.put("archived", documentReader.getInactiveDatabaseSize());
		documentMap.put("status", new HashMap<String, Long>());

		map.put("documents", documentMap);

		Map<String, Object> stageMap = new HashMap<String, Object>();
		map.put("groups", stageMap);

		PipelineReader pipelineReader = getConnector().getPipelineReader();
		stageMap.put("active", getPipelineScanner().getStageConfigMap(pipelineReader.getPipeline()));

		stageMap.put("debug", getPipelineScanner().getStageConfigMap(pipelineReader.getDebugPipeline()));

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
