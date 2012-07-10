package com.findwise.hydra.net;

import static org.junit.Assert.fail;

import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import com.findwise.hydra.common.DocumentFile;
import com.findwise.hydra.local.RemotePipeline;
import com.findwise.hydra.memorydb.MemoryConnector;
import com.findwise.hydra.memorydb.MemoryDocument;
import com.findwise.hydra.memorydb.MemoryType;


public class FileHandlerTest {
	private MemoryConnector mc;
	private RESTServer server;
	
	@Before
	public void setUp() {
		mc = new MemoryConnector();
		server = RESTServer.getNewStartedRESTServer(20000, new HttpRESTHandler<MemoryType>(mc));
	}
	
	@Test
	public void testSave() throws Exception {
		MemoryDocument testDoc = new MemoryDocument();
		mc.getDocumentWriter().insert(testDoc);
		
		RemotePipeline rp = new RemotePipeline("localhost", server.getPort(), "stage");
		
		String content = "adsafgoaiuhgahgo\ndŒŠšasdas";
		String fileName = "test.txt";
		
		rp.saveFile(IOUtils.toInputStream(content, "UTF-8"), fileName, testDoc.getID().toString());
		
		DocumentFile df = mc.getDocumentReader().getDocumentFile(testDoc, fileName);
		
		if(df==null) {
			fail("File was not properly saved");
		}
		
		if(!df.getFileName().equals(fileName)) {
			fail("File had wrong file name");
		}
		String fc = IOUtils.toString(df.getStream(), "UTF-8");
		
		if(!fc.equals(content)) {
			fail("File had wrong contents");
		}
	}
	
	@Test
	public void testFileList() throws Exception {		
		RemotePipeline rp = new RemotePipeline("localhost", server.getPort(), "stage");
		if(rp.getFileNames("")!=null) {
			fail("Got filenames for non-existant document");
		}
		
		MemoryDocument testDoc = new MemoryDocument();
		mc.getDocumentWriter().insert(testDoc);
		
		List<String> list = rp.getFileNames(testDoc.getID().toString());
		
		if(list==null) {
			fail("Got null when requesting filenames for a real document");
		}
		
		if(list.size()!=0) {
			fail("Got non-zero filename list before any files were added");
		}
		
		String content = "adsafgoaiuhgahgo\ndŒŠšasdas";
		String fileName = "test.txt";
		String fileName2 = "test2.txt";
		
		mc.getDocumentWriter().write(new DocumentFile(testDoc.getID(), fileName, IOUtils.toInputStream(content, "UTF-8"), "stage"));
		mc.getDocumentWriter().write(new DocumentFile(testDoc.getID(), fileName2, IOUtils.toInputStream(content, "UTF-8"), "stage"));
		
		list = rp.getFileNames(testDoc.getID().toString());
		if(list.size()!=2) {
			fail("Didn't get two filenames back");
		}
		if(!list.contains(fileName)) {
			fail("Filename list is missing the first file name");
		}
		if(!list.contains(fileName2)) {
			fail("Filename list is missing the second file name");
		}
	}
	
	@Test
	public void testGetFile() throws Exception {		
		RemotePipeline rp = new RemotePipeline("localhost", server.getPort(), "stage");
		if(rp.getFile("id", "file")!=null) {
			fail("Got non-null for non-existant document and non-existant file");
		}
		
		MemoryDocument testDoc = new MemoryDocument();
		mc.getDocumentWriter().insert(testDoc);

		if(rp.getFile(testDoc.getID().toString(), "file")!=null) {
			fail("Got non-null for non-existant file");
		}
		
		String content = "adsafgoaiuhgahgo\ndŒŠšasdas";
		String fileName = "test.txt";
		String content2 = "adsagagasdgarqRE13123AFg da\ndŒŠšasdas";
		String fileName2 = "test2.txt";
		
		mc.getDocumentWriter().write(new DocumentFile(testDoc.getID(), fileName, IOUtils.toInputStream(content, "UTF-8"), "stage"));
		mc.getDocumentWriter().write(new DocumentFile(testDoc.getID(), fileName2, IOUtils.toInputStream(content2, "UTF-8"), "stage"));
		
		InputStream s = rp.getFile(fileName, testDoc.getID().toString());
		if(s==null) {
			fail("Did not get a file stream for file 1");
		}
		String fc = IOUtils.toString(s, "UTF-8");
		if(!fc.equals(content)) {
			System.out.println("::: "+fc);
			System.out.println(content);
			fail("Content of file 1 did not match");
		}
		
		s = rp.getFile(fileName2, testDoc.getID().toString());
		if(s==null) {
			fail("Did not get a file stream for file 2");
		}
		
		if(!IOUtils.toString(s, "UTF-8").equals(content2)) {
			fail("Content of file 2 did not match");
		}
	}
	
	@Test
	public void testDeleteFile() throws Exception {		
		RemotePipeline rp = new RemotePipeline("localhost", server.getPort(), "stage");
		if(rp.deleteFile("name", "id")) {
			fail("Got positive response for non-existant document and non-existant file");
		}
		
		MemoryDocument testDoc = new MemoryDocument();
		mc.getDocumentWriter().insert(testDoc);

		if(rp.getFile("file", testDoc.getID().toString())!=null) {
			fail("Got positive response for non-existant file");
		}
		
		String content = "adsafgoaiuhgahgo\ndŒŠšasdas";
		String fileName = "test.txt";
		String fileName2 = "test2.txt";
		
		mc.getDocumentWriter().write(new DocumentFile(testDoc.getID(), fileName, IOUtils.toInputStream(content, "UTF-8"), "stage"));
		mc.getDocumentWriter().write(new DocumentFile(testDoc.getID(), fileName2, IOUtils.toInputStream(content, "UTF-8"), "stage"));
		
		rp.deleteFile(fileName, testDoc.getID().toString());
		
		if(mc.getDocumentReader().getDocumentFileNames(testDoc).size()!=1) {
			fail("Incorrect amount of files attached to document after delete");
		}
		
		if(!mc.getDocumentReader().getDocumentFileNames(testDoc).get(0).equals(fileName2)) {
			fail("Wrong file left after delete");
		}

		rp.deleteFile(fileName2, testDoc.getID().toString());
		
		if(mc.getDocumentReader().getDocumentFileNames(testDoc).size()!=0) {
			fail("Still some files after both should have been deleted");
		}
	}
}
