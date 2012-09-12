package com.findwise.hydra.mongodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Test;

import com.findwise.hydra.DatabaseFile;
import com.findwise.hydra.TestModule;
import com.google.inject.Guice;
import com.mongodb.Mongo;

public class MongoPipelineIOTest {
	MongoConnector mdc;
	
	private void createAndConnect() throws Exception {
		mdc = Guice.createInjector(new TestModule("junit-MongoPipelineIOTest")).getInstance(MongoConnector.class);
		
		mdc.waitForWrites(true);
		
		mdc.connect();
	}
	
	@AfterClass
	public static void tearDownClass() throws Exception {
		new Mongo().getDB("junit-MongoPipelineIOTest").dropDatabase();
	}
	
	@Test
	public void testSave() throws Exception {
		createAndConnect();
		int count = mdc.getPipelineReader().getFiles().size();
		
		String testString = "testString";
		mdc.getPipelineWriter().save("file.txt", new ByteArrayInputStream(testString.getBytes("UTF-8")));
		if(mdc.getPipelineReader().getFiles().size()!=count+1) {
			fail("Did not save");
		}

		mdc.getPipelineWriter().save("file2.txt", new ByteArrayInputStream("random string".getBytes("UTF-8")));
		if(mdc.getPipelineReader().getFiles().size()!=count+2) {
			fail("Did not save the second file...");
		}
	}
	
	@Test
	public void testDeleteFile() throws Exception {
		testSave();
		
		mdc.getPipelineWriter().save("file", new ByteArrayInputStream("random".getBytes("UTF-8")));
		mdc.getPipelineWriter().save("filename", new ByteArrayInputStream("random2".getBytes("UTF-8")));

		int count = mdc.getPipelineReader().getFiles().size();
		
		List<DatabaseFile> list = mdc.getPipelineReader().getFiles();
		
		mdc.getPipelineWriter().deleteFile(list.get(0).getId());
		assertEquals(mdc.getPipelineReader().getFiles().size(), count-1);
	}

	@Test
	public void testFilename() throws Exception {
		createAndConnect();
		mdc.getPipelineWriter().save("myFile.txt", new ByteArrayInputStream("some file".getBytes("UTF-8")));
		
		List<DatabaseFile> list = mdc.getPipelineReader().getFiles();
		for(DatabaseFile df : list) {
			if(df.getFilename().equals("myFile.txt")) {
				return;
			}
		}
		fail("Did not get correct file name!");
	}
	
	@Test
	public void testGetContents() throws Exception {
		createAndConnect();
		String filename = "testContentsFile.txt";
		String contents = "This is the contents of the file";
		mdc.getPipelineWriter().save(filename, new ByteArrayInputStream(contents.getBytes("UTF-8")));
		
		List<DatabaseFile> list = mdc.getPipelineReader().getFiles();
		for(DatabaseFile df : list) {
			if(df.getFilename().equals(filename)) {
				InputStream is = mdc.getPipelineReader().getStream(df);
				BufferedReader br = new BufferedReader(new InputStreamReader(is));
				assertEquals(br.readLine(), contents);
				return;
			}
		}
		fail("Did not find the file");
	}
	
}
