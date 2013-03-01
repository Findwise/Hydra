package com.findwise.hydra;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.findwise.hydra.local.LocalDocumentID;

@SuppressWarnings("rawtypes")
public class CachingDocumentIOTest {
	DatabaseConnector<?> backing;
	DatabaseConnector<?> caching;
	DocumentReader backingReader;
	DocumentWriter backingWriter;
	DocumentReader cachingReader;
	DocumentWriter cachingWriter;
	DatabaseDocument doc1, doc2, doc3;
	
	DocumentID id1, id2, id3;
	
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
		
		id1 = new LocalDocumentID(1);
		id2 = new LocalDocumentID(2);
		id3 = new LocalDocumentID(3);
		
		Mockito.when(backing.getDocumentReader()).thenReturn(backingReader);
		Mockito.when(backing.getDocumentWriter()).thenReturn(backingWriter);
		Mockito.when(caching.getDocumentReader()).thenReturn(cachingReader);
		Mockito.when(caching.getDocumentWriter()).thenReturn(cachingWriter);
		
		Mockito.when(backingReader.toDocumentIdFromJson("1")).thenReturn(id1);
		Mockito.when(backingReader.toDocumentIdFromJson("2")).thenReturn(id2);
		Mockito.when(backingReader.toDocumentIdFromJson("3")).thenReturn(id3);

		doc1 = Mockito.mock(DatabaseDocument.class);
		Mockito.when(doc1.getID()).thenReturn(new LocalDocumentID(1));
		doc2 = Mockito.mock(DatabaseDocument.class);
		Mockito.when(doc2.getID()).thenReturn(new LocalDocumentID(2));
		doc3 = Mockito.mock(DatabaseDocument.class);
		Mockito.when(doc3.getID()).thenReturn(new LocalDocumentID(3));

		Mockito.when(backing.convert(doc1)).thenReturn(doc1);
		Mockito.when(backing.convert(doc2)).thenReturn(doc2);
		Mockito.when(backing.convert(doc3)).thenReturn(doc3);
		Mockito.when(caching.convert(doc1)).thenReturn(doc1);
		Mockito.when(caching.convert(doc2)).thenReturn(doc2);
		Mockito.when(caching.convert(doc3)).thenReturn(doc3);
		
		io = new CachingDocumentIO(caching, backing);
	}

	@Ignore
	@Test
	public void testGetDocumentById()  {
		Mockito.when(cachingReader.getDocumentById(Mockito.eq(id1))).thenReturn(doc1);
		Mockito.when(cachingReader.getDocumentById(Mockito.eq(id2))).thenReturn(null);
		Mockito.when(cachingReader.getDocumentById(Mockito.eq(id3))).thenReturn(null);
		
		Mockito.when(backingReader.getDocumentById(Mockito.eq(id2), false)).thenReturn(doc2);
		Mockito.when(backingReader.getDocumentById(Mockito.eq(id3), false)).thenReturn(null);
		
		DatabaseDocument<?> d = io.getDocumentById(id1);
		io.getDocumentById(id2);
		
		Mockito.verify(cachingReader, Mockito.times(1)).getDocumentById(id1);
		Mockito.verify(backingReader, Mockito.never()).getDocumentById(id1);
		
		Assert.assertEquals(doc1.getID(), d.getID());
		
		//Should be either of these
		try {
			Mockito.verify(backingReader, Mockito.times(1)).getDocumentById(Mockito.eq(id2));
		} catch(Throwable e) {
			Mockito.verify(backingReader, Mockito.times(1)).getDocumentById(Mockito.eq(id2), false);
		}
		verifyCacheFilledOnce();
		d = io.getDocumentById(id3);
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
		CachingDocumentIO io = new CachingDocumentIO(caching, backing, 10, 0);
		io.prepare();
		
		DatabaseQuery q = Mockito.mock(DatabaseQuery.class);
		Mockito.when(io.convert(q)).thenReturn(q);
		String tag = "t";
		
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put(CachingDocumentIO.CACHED_TIME_METADATA_KEY, new Date());
		Mockito.when(doc1.getMetadataMap()).thenReturn(map);
		
		Mockito.when(cachingWriter.getAndTag(q, tag)).thenReturn(null);

		ArrayList<DatabaseDocument> list = new ArrayList<DatabaseDocument>();
		list.add(doc1);
		Mockito.when(backingWriter.getAndTag(Mockito.eq(q), Mockito.anyString(), Mockito.anyInt())).thenReturn(list);
		
		Mockito.when(cachingReader.getDocuments(Mockito.any(DatabaseQuery.class), Mockito.anyInt())).thenReturn(list);
		
		io.getDocument(q);
		
		verifyCacheFilledOnce();
		
		Thread.sleep(io.getCacheTTL()+1500);
		
		Mockito.verify(cachingWriter, Mockito.never()).delete(doc1);
		Mockito.verify(backingWriter, Mockito.never()).update(doc1);
		
		io.setCacheTTL(100); //Purge cache every 100ms...
		Thread.sleep(1300);
		
		Mockito.verify(cachingWriter, Mockito.times(1)).delete(doc1);
		Mockito.verify(backingWriter, Mockito.times(1)).update(doc1);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testCacheExpirationForFetchedDocument() throws Exception {
		CachingDocumentIO io = new CachingDocumentIO(caching, backing, 10, 0);
		io.prepare();
		
		DatabaseQuery q = Mockito.mock(DatabaseQuery.class);
		Mockito.when(io.convert(q)).thenReturn(q);
		String tag = "t";
		
		Mockito.when(cachingWriter.getAndTag(q, tag)).thenReturn(null);

		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put(CachingDocumentIO.CACHED_TIME_METADATA_KEY, new Date());
		Mockito.when(doc1.getMetadataMap()).thenReturn(map);
		Mockito.when(doc2.getMetadataMap()).thenReturn(map);
		HashSet<String> set = new HashSet<String>();
		set.add("x");
		Mockito.when(doc1.getFetchedBy()).thenReturn(set);
		Mockito.when(doc1.getFetchedTime(Mockito.eq("x"))).thenReturn(new Date());
		Mockito.when(doc2.getFetchedBy()).thenReturn(set);
		Mockito.when(doc2.getFetchedTime(Mockito.eq("x"))).thenReturn(new Date(System.currentTimeMillis()+100000)); //Fetched in the future

		ArrayList<DatabaseDocument> list = new ArrayList<DatabaseDocument>();
		list.add(doc1);
		list.add(doc2);
		
		Mockito.when(backingWriter.getAndTag(Mockito.eq(q), Mockito.anyString(), Mockito.anyInt())).thenReturn(list);
		
		Mockito.when(cachingReader.getDocuments(Mockito.any(DatabaseQuery.class), Mockito.anyInt())).thenReturn(list);
		
		io.getDocument(q);
		
		verifyCacheFilledOnce();
		
		Thread.sleep(io.getCacheTTL()+1500);
		
		Mockito.verify(cachingWriter, Mockito.never()).delete(doc1);
		Mockito.verify(backingWriter, Mockito.never()).update(doc1);

		io.setCacheTTL(100); //Purge cache every 100ms...
		Thread.sleep(1300);
		
		Mockito.verify(cachingWriter, Mockito.times(1)).delete(doc1);
		Mockito.verify(backingWriter, Mockito.times(1)).update(doc1);
		Mockito.verify(cachingWriter, Mockito.never()).delete(doc2);
		Mockito.verify(backingWriter, Mockito.never()).update(doc2);
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
