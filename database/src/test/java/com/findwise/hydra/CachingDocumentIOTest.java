package com.findwise.hydra;

import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

@SuppressWarnings("rawtypes")
public class CachingDocumentIOTest {
	DatabaseConnector<?> backing;
	DatabaseConnector<?> caching;
	DocumentReader backingReader;
	DocumentWriter backingWriter;
	DocumentReader cachingReader;
	DocumentWriter cachingWriter;
	DatabaseDocument doc1, doc2, doc3;
	
	CachingDocumentIO<?, ?> io;
	
	@SuppressWarnings("unchecked")
	@Before
	public void setup() throws Exception {
		backing = Mockito.mock(DatabaseConnector.class);
		caching = Mockito.mock(DatabaseConnector.class);
		
		backingReader = Mockito.mock(DocumentReader.class);
		backingWriter = Mockito.mock(DocumentWriter.class);
		cachingReader = Mockito.mock(DocumentReader.class);
		cachingWriter = Mockito.mock(DocumentWriter.class);
		
		Mockito.when(backing.getDocumentReader()).thenReturn(backingReader);
		Mockito.when(backing.getDocumentWriter()).thenReturn(backingWriter);
		Mockito.when(caching.getDocumentReader()).thenReturn(cachingReader);
		Mockito.when(caching.getDocumentWriter()).thenReturn(cachingWriter);

		doc1 = Mockito.mock(DatabaseDocument.class);
		Mockito.when(doc1.getID()).thenReturn(1);
		doc2 = Mockito.mock(DatabaseDocument.class);
		Mockito.when(doc2.getID()).thenReturn(2);
		doc3 = Mockito.mock(DatabaseDocument.class);
		Mockito.when(doc3.getID()).thenReturn(3);
		
		io = new CachingDocumentIO(caching, backing);
	}

	@Test
	public void testGetDocumentById()  {
		Mockito.when(cachingReader.getDocumentById(1)).thenReturn(doc1);
		Mockito.when(cachingReader.getDocumentById(2)).thenReturn(null);
		Mockito.when(cachingReader.getDocumentById(3)).thenReturn(null);
		
		Mockito.when(backingReader.toDocumentIdFromJson("2")).thenReturn(2);
		Mockito.when(backingReader.toDocumentIdFromJson("3")).thenReturn(3);
		Mockito.when(backingReader.getDocumentById(2, false)).thenReturn(doc2);
		Mockito.when(backingReader.getDocumentById(3, false)).thenReturn(null);
		
		DatabaseDocument<?> d = io.getDocumentById(1);
		io.getDocumentById(2);
		
		Mockito.verify(cachingReader, Mockito.times(1)).getDocumentById(1);
		Mockito.verify(backingReader, Mockito.never()).getDocumentById(1);
		
		Assert.assertEquals(doc1.getID(), d.getID());
		
		//Should be either of these
		try {
			Mockito.verify(backingReader, Mockito.times(1)).getDocumentById(2);
		} catch(Throwable e) {
			Mockito.verify(backingReader, Mockito.times(1)).getDocumentById(2, false);
		}
		verifyCacheFilledOnce();
		d = io.getDocumentById(3);
		verifyCacheFilledOnce();
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testGetDocument() {
		DatabaseQuery q1 = Mockito.mock(DatabaseQuery.class);
		DatabaseQuery q2 = Mockito.mock(DatabaseQuery.class);
		String tag = "t";
		
		Mockito.when(cachingWriter.getAndTag(q1, tag)).thenReturn(doc1);
		Mockito.when(cachingWriter.getAndTag(q2, tag)).thenReturn(null);
		
		Mockito.when(io.convert(q2)).thenReturn(q2);
		
		ArrayList<DatabaseDocument> list = new ArrayList<DatabaseDocument>();
		list.add(doc2);
		Mockito.when(backingWriter.getAndTag(Mockito.eq(q2), Mockito.anyString(), Mockito.anyInt())).thenReturn(list);
		
		Assert.assertEquals(doc1.getID(), io.getAndTag(q1, tag).getID());
		Mockito.verify(backingWriter, Mockito.times(0)).getAndTag(Mockito.eq(q2), Mockito.anyString(), Mockito.anyInt());
		
		io.getAndTag(q2, tag);
		
		Mockito.verify(backingWriter, Mockito.times(1)).getAndTag(Mockito.eq(q2), Mockito.anyString(), Mockito.anyInt());
		verifyCacheFilledOnce();
		Mockito.verify(cachingWriter, Mockito.times(2)).getAndTag(Mockito.eq(q2), Mockito.anyString());
	}
	
	@SuppressWarnings("unchecked")
	private void verifyCacheFilledOnce() {
		Mockito.verify(cachingWriter, Mockito.atLeast(1)).update(Mockito.any(DatabaseDocument.class));
	}
}
