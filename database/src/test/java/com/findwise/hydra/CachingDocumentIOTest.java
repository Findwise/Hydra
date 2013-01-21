package com.findwise.hydra;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.findwise.hydra.common.DocumentFile;

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
		
		Mockito.when(backingReader.toDocumentIdFromJson("1")).thenReturn(1);
		Mockito.when(backingReader.toDocumentIdFromJson("2")).thenReturn(2);
		Mockito.when(backingReader.toDocumentIdFromJson("3")).thenReturn(3);

		doc1 = Mockito.mock(DatabaseDocument.class);
		Mockito.when(doc1.getID()).thenReturn(1);
		doc2 = Mockito.mock(DatabaseDocument.class);
		Mockito.when(doc2.getID()).thenReturn(2);
		doc3 = Mockito.mock(DatabaseDocument.class);
		Mockito.when(doc3.getID()).thenReturn(3);

		Mockito.when(backing.convert(doc1)).thenReturn(doc1);
		Mockito.when(backing.convert(doc2)).thenReturn(doc2);
		Mockito.when(backing.convert(doc3)).thenReturn(doc3);
		
		io = new CachingDocumentIO(caching, backing);
	}

	@Test
	public void testGetDocumentById()  {
		Mockito.when(cachingReader.getDocumentById(1)).thenReturn(doc1);
		Mockito.when(cachingReader.getDocumentById(2)).thenReturn(null);
		Mockito.when(cachingReader.getDocumentById(3)).thenReturn(null);
		
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
		Mockito.when(io.convert(q2)).thenReturn(q2);
		String tag = "t";
		
		Mockito.when(cachingWriter.getAndTag(q1, tag)).thenReturn(doc1);
		Mockito.when(cachingWriter.getAndTag(q2, tag)).thenReturn(null);
		
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
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testGetDocumentFileNamesCached() {
		ArrayList<String> cache = new ArrayList<String>();
		cache.add("x");
		cache.add("y");
		
		Mockito.when(cachingReader.getDocumentFileNames(doc1)).thenReturn(cache);
		
		List<String> l = io.getDocumentFileNames(doc1);
		verifyInList(l, "x", "y");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testGetDocumentFileNamesPartiallyCached() {
		ArrayList<String> cache = new ArrayList<String>();
		cache.add("x");
		Mockito.when(cachingReader.getDocumentFileNames(doc1)).thenReturn(cache);
		ArrayList<String> back = new ArrayList<String>();
		back.add("x");
		back.add("y");
		Mockito.when(backingReader.getDocumentFileNames(doc1)).thenReturn(back);

		List<String> l = io.getDocumentFileNames(doc1);
		verifyInList(l, "x", "y");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testGetDocumentFileNamesNotCached() {
		ArrayList<String> cache = new ArrayList<String>();
		Mockito.when(cachingReader.getDocumentFileNames(doc1)).thenReturn(cache);
		ArrayList<String> back = new ArrayList<String>();
		back.add("x");
		back.add("y");
		Mockito.when(backingReader.getDocumentFileNames(doc1)).thenReturn(back);

		List<String> l = io.getDocumentFileNames(doc1);
		verifyInList(l, "x", "y");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testGetDocumentFileCached() throws IOException {
		ArrayList<String> cache = new ArrayList<String>();
		cache.add("x");
		cache.add("y");
		Mockito.when(cachingReader.getDocumentFileNames(doc1)).thenReturn(cache);
		Mockito.when(cachingReader.getDocumentFile(doc1, "x")).thenReturn(null);
		
		ArrayList<String> back = new ArrayList<String>();
		DocumentFile x = new DocumentFile(doc1.getID(), "x", IOUtils.toInputStream("content"));
		DocumentFile y = new DocumentFile(doc1.getID(), "y", IOUtils.toInputStream("content"));
		Mockito.when(backingReader.getDocumentFileNames(doc1)).thenReturn(back);
		Mockito.when(cachingReader.getDocumentFile(doc1, "x")).thenReturn(x);
		Mockito.when(cachingReader.getDocumentFile(doc1, "y")).thenReturn(y);

		Assert.assertEquals("x", io.getDocumentFile(doc1, "x").getFileName());
		Assert.assertEquals("y", io.getDocumentFile(doc1, "y").getFileName());
		Mockito.verify(cachingWriter, Mockito.times(0)).write(x);
		Mockito.verify(cachingWriter, Mockito.times(0)).write(y);
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testGetDocumentFileNotCached() throws IOException {
		ArrayList<String> cache = new ArrayList<String>();
		Mockito.when(cachingReader.getDocumentFileNames(doc1)).thenReturn(cache);
		Mockito.when(cachingReader.getDocumentFile(doc1, "x")).thenReturn(null);
		
		ArrayList<String> back = new ArrayList<String>();
		back.add("x");
		back.add("y");
		DocumentFile x = new DocumentFile(doc1.getID(), "x", IOUtils.toInputStream("content"));
		DocumentFile y = new DocumentFile(doc1.getID(), "y", IOUtils.toInputStream("content"));
		Mockito.when(backingReader.getDocumentFileNames(doc1)).thenReturn(back);
		Mockito.when(backingReader.getDocumentFile(doc1, "x")).thenReturn(x);
		Mockito.when(backingReader.getDocumentFile(doc1, "y")).thenReturn(y);
		Mockito.when(backingReader.getDocumentFile(doc1, "z")).thenReturn(null);

		io.getDocumentFile(doc1, "x");
		io.getDocumentFile(doc1, "y");
		io.getDocumentFile(doc1, "z");
		Mockito.verify(cachingWriter, Mockito.times(1)).write(x);
		Mockito.verify(cachingWriter, Mockito.times(1)).write(y);
		Mockito.verify(cachingWriter, Mockito.times(0)).write(null);
		Mockito.verify(cachingReader, Mockito.times(2)).getDocumentFile(doc1, "x");
		Mockito.verify(cachingReader, Mockito.times(2)).getDocumentFile(doc1, "y");
		Mockito.verify(cachingReader, Mockito.times(1)).getDocumentFile(doc1, "z");
		Mockito.verify(backingReader, Mockito.times(1)).getDocumentFile(doc1, "z");
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testCacheExpiration() throws Exception {
		
		DatabaseQuery q = Mockito.mock(DatabaseQuery.class);
		Mockito.when(io.convert(q)).thenReturn(q);
		String tag = "t";
		
		Mockito.when(cachingWriter.getAndTag(q, tag)).thenReturn(null);

		ArrayList<DatabaseDocument> list = new ArrayList<DatabaseDocument>();
		list.add(doc1);
		Mockito.when(backingWriter.getAndTag(Mockito.eq(q), Mockito.anyString(), Mockito.anyInt())).thenReturn(list);
		
		io.getDocument(q);
		
		verifyCacheFilledOnce();
		
		Thread.sleep(io.getCacheTTL()+1000);
		
		Mockito.verify(cachingWriter, Mockito.times(1)).delete(doc1);
		Mockito.verify(backingWriter, Mockito.times(1)).update(doc1);
		
	}
	
	private void verifyInList(Collection<String> list, String ... os) {
		for(String o : os) {
			if(!list.contains(o)) {
				Assert.fail("String '"+o+"' is not in list: "+list);
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	private void verifyCacheFilledOnce() {
		Mockito.verify(cachingWriter, Mockito.atLeast(1)).update(Mockito.any(DatabaseDocument.class));
	}
}
