package com.findwise.hydra.admin;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.findwise.hydra.DatabaseFile;
import com.findwise.hydra.PipelineReader;

/**
 * Creates temporary library JARs that can be read via URLs
 * 
 * @author olof.nilsson
 * 
 */
public class JarFetcher {

	private final String tempPrefix;

	private final String tempSuffix;

	public JarFetcher() {
		this("hydra-admin-tmp", ".jar");
	}

	public JarFetcher(String prefix, String suffix) {
		this.tempPrefix = prefix;
		this.tempSuffix = suffix;
	}

	/**
	 * TODO: Avoid creating new files for each call to this method
	 * 
	 * @param tmpDir
	 *            directory for writing jars to
	 * @param pipelineReader
	 * @param files
	 * @return urls to jars loaded for this pipeline
	 * @throws IOException
	 */
	public List<URL> getJars(File tmpDir, PipelineReader pipelineReader,
			DatabaseFile... files) throws IOException {
		List<URL> urls = new ArrayList<URL>();

		for (DatabaseFile df : files) {
			File tmpFile = File.createTempFile(tempPrefix, tempSuffix, tmpDir);

			FileUtils.copyInputStreamToFile(pipelineReader.getStream(df),
					tmpFile);
			URL url = tmpFile.toURI().toURL();
			urls.add(url);
		}
		return urls;
	}

	public void cleanup(List<URL> urls) {
		for (URL url : urls) {
			File file = FileUtils.toFile(url);
			FileUtils.deleteQuietly(file);
		}
	}
}
