package com.findwise.hydra.admin;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.DatabaseFile;
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.Pipeline;
import com.findwise.hydra.PipelineReader;
import com.findwise.hydra.Stage;
import com.findwise.hydra.StageGroup;

public class PipelineScanner<T extends DatabaseType> {
	private Logger logger = LoggerFactory.getLogger(PipelineScanner.class);

	private final PipelineReader pipelineReader;

	private final JarFetcher jarFetcher;

	private final StageScanner stageScanner;

	public PipelineScanner(PipelineReader pipelineReader) {
		this(pipelineReader, new JarFetcher(), new StageScanner());
	}

	public PipelineScanner(PipelineReader pipelineReader, JarFetcher jarFetcher, StageScanner stageScanner) {
		this.pipelineReader = pipelineReader;
		this.jarFetcher = jarFetcher;
		this.stageScanner = stageScanner;
	}

	public List<DatabaseFile> getLibraryFiles() {
		return pipelineReader.getFiles();
	}

	/**
	 * Extract all stages that can be configured from the supplied library
	 * 
	 * @param df
	 *            library in the database
	 * @return map of stage class names to their representation
	 */
	public Map<String, StageInformation> getStagesMap(DatabaseFile df) {
		Map<String, StageInformation> map = new HashMap<String, StageInformation>();
		try {
			List<URL> jars = jarFetcher.getJars(FileUtils.getTempDirectory(), pipelineReader, df);
			
			for (Class<?> c : stageScanner.getClasses(jars)) {
				try {
					StageInformation stageInformation = new StageInformation(c);
					String stageClass = null != c.getCanonicalName() ? c.getCanonicalName() : c.getName();
					map.put(stageClass, stageInformation);
				} catch (NoSuchElementException e) {
					logger.error("Unable to get stage information for class "
							+ c.getCanonicalName(), e);
				}
			}
			jarFetcher.cleanup(jars);
		} catch (IOException e) {
			logger.error("Unable to get stage classes", e);
		}
		return map;
	}

	/**
	 * Extracts all currently configured groups and stages from the supplied
	 * pipeline
	 * 
	 * @param pipeline
	 * @return map containing groups of stages
	 */
	public Map<String, Object> getStageConfigMap(Pipeline pipeline) {
		Map<String, Object> map = new HashMap<String, Object>();
		for (StageGroup g : pipeline.getStageGroups()) {
			HashMap<String, Object> group = new HashMap<String, Object>();
			HashMap<String, Object> stages = new HashMap<String, Object>();
			for (Stage s : g.getStages()) {
				HashMap<String, Object> stage = new HashMap<String, Object>();
				stage.put("properties", s.getProperties());

				HashMap<String, Object> file = new HashMap<String, Object>();
				file.put("id", s.getDatabaseFile().getId());
				file.put("name", s.getDatabaseFile().getFilename());
				stage.put("file", file);
				stages.put(s.getName(), stage);
			}
			group.put("properties", getMapWithoutDefaults(g));
			group.put("stages", stages);
			map.put(g.getName(), group);
		}
		return map;
	}

	private Map<String, Object> getMapWithoutDefaults(StageGroup group) {
		Map<String, Object> propertiesMap = group.toPropertiesMap();
		Iterator<Map.Entry<String, Object>> it = propertiesMap.entrySet()
				.iterator();
		while (it.hasNext()) {
			if (it.next().getValue() == null) {
				it.remove();
			}
		}
		if (group.getRetries() == -1) {
			propertiesMap.remove(StageGroup.RETRIES_KEY);
		}
		if (!group.isLogging()) {
			propertiesMap.remove(StageGroup.LOGGING_KEY);
		}
		return propertiesMap;
	}
}
