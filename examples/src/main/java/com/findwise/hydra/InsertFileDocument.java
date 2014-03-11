package com.findwise.hydra;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.http.HttpException;

import com.findwise.hydra.local.Local;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.LocalQuery;
import com.findwise.hydra.local.RemotePipeline;

public class InsertFileDocument {
	public void start() throws IOException, JsonException, HttpException, URISyntaxException {
		postDocuments(1);
	}
	
	public void postDocuments(int numberToPost) throws JsonException, IOException, HttpException, URISyntaxException {
		RemotePipeline rp = new RemotePipeline("insertStage");
		for(int i=0; i<numberToPost; i++) {
			rp.saveFull(LocalDocumentFactory.getRandomStringDocument("in", "id"));
			RemotePipeline rp2 = new RemotePipeline("fileAdder");
			LocalDocument ld = rp2.getDocument(new LocalQuery());
			File f = getFile();
			FileInputStream fis = new FileInputStream(f);
			DocumentFile<Local> df = new DocumentFile<Local>(ld.getID(), f.getName(), fis);
			df.setEncoding(new InputStreamReader(df.getStream()).getEncoding());
			df.setMimetype("application/msword");
			rp2.saveFile(df);
		}
	}
	
	public File getFile() throws URISyntaxException {
		URL path = ClassLoader.getSystemResource("goodapi.doc");
		return new File(path.toURI());
	}
	
	public static void main(String[] args) throws Exception {
		new InsertFileDocument().start();
	}
}
