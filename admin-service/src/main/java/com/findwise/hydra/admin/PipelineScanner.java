package com.findwise.hydra.admin;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.reflections.Reflections;

import com.findwise.hydra.DatabaseFile;
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.PipelineReader;

public class PipelineScanner<T extends DatabaseType> {
	private PipelineReader<T> pipelineReader;
	
	public PipelineScanner(PipelineReader<T> reader) {
		pipelineReader = reader;
	}
	
	public List<DatabaseFile> getLibraryFiles() {
		return pipelineReader.getFiles();
	}

	public List<Class<?>> getStageClasses(File tmpdir, DatabaseFile ... files) throws IOException {
		List<URL> urls = new ArrayList<URL>();
		
		for(DatabaseFile df : files) {
			URL url = new File(tmpdir.getAbsolutePath()+File.separator+df.getFilename()).toURI().toURL();

			FileUtils.copyInputStreamToFile(pipelineReader.getStream(df), new File(url.getFile()));
			urls.add(url);
		}
		
		ClassLoader cl = new URLClassLoader(urls.toArray(new URL[urls.size()]));
		
		return new StageScanner().getClasses(new Reflections(urls, cl));
	}
}
