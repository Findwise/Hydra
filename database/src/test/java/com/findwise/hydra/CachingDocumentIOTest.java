package com.findwise.hydra;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class CachingDocumentIOTest {
	DatabaseConnector<?> backing;
	DatabaseConnector<?> caching;
	DocumentReader backingReader;
	DocumentReader cachingReader;
	DocumentWriter cachingWriter;
	DatabaseDocument doc1, doc2, doc3;
	
	
	@Before
	public void setup() {
		backing = Mockito.mock(DatabaseConnector.class);
		caching = Mockito.mock(DatabaseConnector.class);
		
		backingReader = Mockito.mock(DocumentReader.class);
		cachingReader = Mockito.mock(DocumentReader.class);
		cachingWriter = Mockito.mock(DocumentWriter.class);
		
		Mockito.when(backing.getDocumentReader()).thenReturn(backingReader);
		Mockito.when(caching.getDocumentReader()).thenReturn(cachingReader);
		Mockito.when(caching.getDocumentWriter()).thenReturn(cachingWriter);

		doc1 = Mockito.mock(DatabaseDocument.class);
		Mockito.when(doc1.getID()).thenReturn(1);
		doc2 = Mockito.mock(DatabaseDocument.class);
		Mockito.when(doc2.getID()).thenReturn(2);
		doc3 = Mockito.mock(DatabaseDocument.class);
		Mockito.when(doc3.getID()).thenReturn(3);
	}
	

	@Test
	public void testGetDocumentById() throws Exception {
		Mockito.when(cachingReader.getDocumentById(1)).thenReturn(doc1);
		Mockito.when(cachingReader.getDocumentById(2)).thenReturn(null);
		
		CachingDatabaseConnector<?, ?> cdc = new CachingDatabaseConnector(backing, caching);
		cdc.connect();

		cdc.getDocumentReader().getDocumentById(1);
		cdc.getDocumentReader().getDocumentById(2);
		
		//Should be either of these
		try {
			Mockito.verify(backingReader, Mockito.times(1)).getDocumentById(2);
		} catch(Throwable e) {
			Mockito.verify(backingReader, Mockito.times(1)).getDocumentById(2, false);
		}
	}

}
