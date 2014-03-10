package com.findwise.hydra.mongodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import com.findwise.hydra.DatabaseFile;
import com.findwise.hydra.Pipeline;
import com.findwise.hydra.Stage;
import com.findwise.hydra.StageGroup;
import com.mongodb.WriteConcern;

public class MongoPipelineIOIT {
	
	@Rule
	public MongoConnectorResource mongoConnectorResource = new MongoConnectorResource(getClass());
	
	@Test
	public void testSave() throws Exception {
		MongoConnector mdc = mongoConnectorResource.getConnector();
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
		MongoConnector mdc = mongoConnectorResource.getConnector();
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
		MongoConnector mdc = mongoConnectorResource.getConnector();
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
		MongoConnector mdc = mongoConnectorResource.getConnector();
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
		MongoConnector mdc = mongoConnectorResource.getConnector();
		Pipeline p = new Pipeline();
		StageGroup singleGroup = new StageGroup("singleStage");
		StageGroup multiGroup = new StageGroup("multi");
		Stage single = new Stage("singleStage", new DatabaseFile());
		Stage multi1 = new Stage("multi1", new DatabaseFile());
		Stage multi2 = new Stage("multi2", new DatabaseFile());
		multiGroup.addStage(multi1);
		multiGroup.addStage(multi2);
		singleGroup.addStage(single);
		
		p.addGroup(multiGroup);
		p.addGroup(singleGroup);
		
		assertEquals(2, p.getStageGroups().size());
		assertTrue(p.hasGroup("multi"));
		assertTrue(p.hasGroup("singleStage"));
		
		mongoConnectorResource.reset();
		mdc = mongoConnectorResource.getConnector();
		MongoPipelineReader reader = new MongoPipelineReader(mdc.getDB());
		MongoPipelineWriter writer = new MongoPipelineWriter(reader, WriteConcern.SAFE);
		
		assertEquals(0, reader.getPipeline().getStages().size());
		writer.write(p);
		
		assertEquals(3, reader.getPipeline().getStages().size());

		assertTrue(reader.getPipeline().hasGroup("multi"));
		assertTrue(reader.getPipeline().hasGroup("singleStage"));

		assertEquals(2, reader.getPipeline().getGroup("multi").getSize());
		assertEquals(1, reader.getPipeline().getGroup("singleStage").getSize());
	}
	
	@Test
	public void testStageGroupProperties() throws Exception {
		MongoConnector mdc = mongoConnectorResource.getConnector();
		Pipeline p = new Pipeline();
		StageGroup singleGroup = new StageGroup("singleStage");
		StageGroup multiGroup = new StageGroup("multi");
		Stage single = new Stage("singleStage", new DatabaseFile());
		Stage multi1 = new Stage("multi1", new DatabaseFile());
		Stage multi2 = new Stage("multi2", new DatabaseFile());
		multiGroup.addStage(multi1);
		multiGroup.addStage(multi2);
		singleGroup.addStage(single);
		
		p.addGroup(multiGroup);
		p.addGroup(singleGroup);
		
		multiGroup.setJvmParameters("jvm");
		multiGroup.setRetries(3);
		multiGroup.setLogging(false);
		multiGroup.setCmdlineArgs("cmd");
		
		mongoConnectorResource.reset();
		mdc = mongoConnectorResource.getConnector();
		MongoPipelineReader reader = new MongoPipelineReader(mdc.getDB());
		MongoPipelineWriter writer = new MongoPipelineWriter(reader, WriteConcern.SAFE);
		
		writer.write(p);
		
		StageGroup g2 = reader.getPipeline().getGroup("multi");
		assertEquals(multiGroup.getJvmParameters(), g2.getJvmParameters());
		assertEquals(multiGroup.getRetries(), g2.getRetries());
		assertEquals(multiGroup.isLogging(), g2.isLogging());
		assertEquals(multiGroup.getCmdlineArgs(), g2.getCmdlineArgs());
	}
}
