package com.findwise.hydra.net;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.findwise.hydra.local.HttpRemotePipeline;
import org.junit.Before;
import org.junit.Test;

import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.DocumentReader;
import com.findwise.hydra.DocumentWriter;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.LocalDocumentID;
import com.findwise.hydra.local.RemotePipeline;

public class WriteHandlerTest {
	@SuppressWarnings("rawtypes")
	private DatabaseConnector dbc;
	private DocumentWriter<?> writer;
	@SuppressWarnings("rawtypes")
	private DocumentReader reader;
	private RESTServer server;
	
	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception {
		writer = mock(DocumentWriter.class);
		reader = mock(DocumentReader.class);
		dbc = mock(DatabaseConnector.class);
		when(dbc.getDocumentWriter()).thenReturn(writer);
		when(dbc.getDocumentReader()).thenReturn(reader);
		server = RESTServer.getNewStartedRESTServer(14000, new HttpRESTHandler<DatabaseType>(dbc));
		
		
	}

	@SuppressWarnings({ "unchecked"})
	@Test
	public void testSaveFull() throws Exception {
		when(writer.insert(any(DatabaseDocument.class))).thenReturn(false);
		
		DatabaseDocument<?> dbdoc = mock(DatabaseDocument.class);
		when(dbc.convert(any(LocalDocument.class))).thenReturn(dbdoc);
		
		when(dbdoc.getID()).thenReturn(null);
		
		RemotePipeline rp = new HttpRemotePipeline("localhost", server.getPort(), "stage");
		LocalDocument ld = new LocalDocument();
		
		boolean result = rp.saveFull(ld);
		
		if(result) {
			fail("Did not get false response");
		}
		
		verify(writer, times(1)).insert(any(DatabaseDocument.class));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testSave() throws Exception {
		when(writer.insert(any(DatabaseDocument.class))).thenReturn(false);
		
		DatabaseDocument dbdoc = mock(DatabaseDocument.class);
		when(dbc.convert(any(LocalDocument.class))).thenReturn(dbdoc);
		
		LocalDocumentID id = new LocalDocumentID(1);
		
		when(dbdoc.getID()).thenReturn(id);
		when(reader.getDocumentById(id)).thenReturn(dbdoc);
		
		RemotePipeline rp = new HttpRemotePipeline("localhost", server.getPort(), "stage");
		LocalDocument ld = new LocalDocument();
		
		boolean result = rp.saveFull(ld);
		
		if(result) {
			fail("Did not get false response on saveFull");
		}
		
		result = rp.save(ld);
		
		if(result) {
			fail("Did not get false response on save");
		}
		
		verify(writer).update(any(DatabaseDocument.class));
	}

}
