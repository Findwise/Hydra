package com.findwise.hydra.mongodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;

import com.findwise.hydra.DatabaseFile;
import com.findwise.hydra.Pipeline;
import com.findwise.hydra.Stage;
import com.findwise.hydra.StageGroup;
import com.findwise.hydra.TestModule;
import com.google.inject.Guice;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;

public class MongoPipelineIOTest {
	MongoConnector mdc;
	
	private void createAndConnect() throws Exception {
		mdc = Guice.createInjector(new TestModule("junit-MongoPipelineIOTest")).getInstance(MongoConnector.class);
		
		mdc.waitForWrites(true);
		
		mdc.connect();
	}
	
	@After
	public void tearDown() throws Exception {
		tearDownClass();
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
				br.close();
				return;
			}
		}
		fail("Did not find the file");
	}
	
	@Test
	public void testGetStageGroups() throws Exception {
		Pipeline p = new Pipeline();
		Stage single = new Stage("singleStage", new DatabaseFile());
		Stage multi1 = new Stage("multi1", new DatabaseFile());
		Stage multi2 = new Stage("multi2", new DatabaseFile());
		multi1.setGroupName("multi");
		multi2.setGroupName("multi");
		
		p.addStage(single);
		p.addStage(multi1);
		p.addStage(multi2);
		
		Assert.assertEquals(2, p.getStageGroups().size());
		Assert.assertTrue(p.hasStageGroup("multi"));
		Assert.assertTrue(p.hasStageGroup("singleStage"));
		
		createAndConnect();
		MongoPipelineReader reader = new MongoPipelineReader(mdc.getDB());
		MongoPipelineWriter writer = new MongoPipelineWriter(reader, WriteConcern.SAFE);
		
		Assert.assertEquals(0, reader.getPipeline().getStages().size());
		writer.write(p);
		
		Assert.assertEquals(3, reader.getPipeline().getStages().size());

		Assert.assertTrue(reader.getPipeline().hasStageGroup("multi"));
		Assert.assertTrue(reader.getPipeline().hasStageGroup("singleStage"));

		Assert.assertEquals(2, reader.getPipeline().getStages("multi").size());
		Assert.assertEquals(1, reader.getPipeline().getStages("singleStage").size());
	}
	
	@Test
	public void testStageGroupProperties() throws Exception {
		Pipeline p = new Pipeline();
		Stage single = new Stage("singleStage", new DatabaseFile());
		Stage multi1 = new Stage("multi1", new DatabaseFile());
		Stage multi2 = new Stage("multi2", new DatabaseFile());
		multi1.setGroupName("multi");
		multi2.setGroupName("multi");
		
		p.addStage(single);
		p.addStage(multi1);
		p.addStage(multi2);
		
		StageGroup g = p.getStageGroup("multi");
		
		g.setJvmParameters("jvm");
		g.setRetries(3);
		g.setLogging(false);
		g.setCmdlineArgs("cmd");
		
		createAndConnect();
		MongoPipelineReader reader = new MongoPipelineReader(mdc.getDB());
		MongoPipelineWriter writer = new MongoPipelineWriter(reader, WriteConcern.SAFE);
		
		writer.write(p);
		
		StageGroup g2 = reader.getPipeline().getStageGroup("multi");
		Assert.assertEquals(g.getJvmParameters(), g2.getJvmParameters());
		Assert.assertEquals(g.getRetries(), g2.getRetries());
		Assert.assertEquals(g.isLogging(), g2.isLogging());
		Assert.assertEquals(g.getCmdlineArgs(), g2.getCmdlineArgs());
	}
}
