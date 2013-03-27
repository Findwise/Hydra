package com.findwise.hydra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.LocalQuery;

@SuppressWarnings({"unchecked", "rawtypes"})
@RunWith(MockitoJUnitRunner.class)
public class CachingDocumentNIOTest {
	public static interface TestType extends DatabaseType {
	}

	@Mock
	Cache<TestType> cache;

	@Mock
	DatabaseConnector<TestType> connector;
	@Mock
	DocumentWriter<TestType> writer;
	@Mock
	DocumentReader<TestType> reader;

	@Mock
	DatabaseQuery<TestType> q1, q2;

	@Mock
	DatabaseDocument<TestType> doc1, doc2;

	@Mock
	DocumentID<TestType> id1;

	private CachingDocumentNIO<TestType> io;

	@Before
	public void setUp() {
		when(connector.getDocumentReader()).thenReturn(reader);
		when(connector.getDocumentWriter()).thenReturn(writer);
		io = new CachingDocumentNIO<CachingDocumentNIOTest.TestType>(connector,
				cache, false);

		when(doc1.getID()).thenReturn(id1);
		when(doc1.copy()).thenReturn(doc1);
		when(doc2.copy()).thenReturn(doc2);
	}

	@Test
	public void testGetAndTagNoHits() {
		when(writer.getAndTag(q1, "tag")).thenReturn(null);
		when(cache.getAndTag(q1, "tag")).thenReturn(null);

		assertNull(io.getAndTag(q1, "tag"));

		verify(cache, never()).add(any(DatabaseDocument.class));
		verify(cache, atLeastOnce()).getAndTag(q1, "tag");
		verify(writer, atLeastOnce()).getAndTag(eq(q1), (String[]) Mockito.anyVararg());
	}

	@Test
	public void testGetAndTagCacheHit() {
		when(writer.getAndTag(q1, "tag")).thenReturn(null);
		when(cache.getAndTag(q1, "tag")).thenReturn(doc1);

		assertEquals(doc1, io.getAndTag(q1, "tag"));

		verify(cache, never()).add(any(DatabaseDocument.class));
		verify(cache, atLeastOnce()).getAndTag(q1, "tag");
		verify(writer, never()).getAndTag(eq(q1), anyInt(),  (String[]) Mockito.anyVararg());
	}

	@Test
	public void testGetAndTagCacheMiss() {
		when(cache.getAndTag(eq(q1), (String[]) Mockito.anyVararg())).thenReturn(null);
		when(writer.getAndTag(eq(q1), (String[]) Mockito.anyVararg())).thenReturn(doc1);

		assertEquals(doc1, io.getAndTag(q1, "tag"));

		InOrder inOrder = inOrder(cache, writer);
		inOrder.verify(cache, times(1)).getAndTag(q1, "tag");
		inOrder.verify(writer, times(1)).getAndTag(eq(q1), (String[]) Mockito.anyVararg());
		inOrder.verify(cache).add(doc1);
		verify(doc1).setFetchedBy(eq("tag"), any(Date.class));
	}

	@Test
	public void testGetAndTagDatabaseQueryWithLimitCacheMiss() {
		Collection<DatabaseDocument<TestType>> c = new ArrayList<DatabaseDocument<TestType>>();
		c.add(doc1);

		when(cache.getAndTag(eq(q1), eq(3), (String[]) Mockito.anyVararg())).thenReturn(null);
		when(writer.getAndTag(eq(q1), eq(3), (String[]) Mockito.anyVararg())).thenReturn(c);
		
		c = io.getAndTag(q1, 3, "tag");
		assertEquals(1, c.size());
		assertEquals(doc1, c.iterator().next());

		InOrder inOrder = inOrder(cache, writer);
		inOrder.verify(cache, times(1)).getAndTag(q1, 3, "tag");
		inOrder.verify(writer, times(1)).getAndTag(eq(q1), anyInt(), (String[]) Mockito.anyVararg());
		inOrder.verify(cache).add(c);
	}
	
	@Test
	public void testGetAndTagDatabaseQueryWithLimitCacheHit() {
		Collection<DatabaseDocument<TestType>> c = new ArrayList<DatabaseDocument<TestType>>();
		c.add(doc1);

		when(cache.getAndTag(q1, 3, "tag")).thenReturn(c);
		
		c = io.getAndTag(q1, 3, "tag");
		assertEquals(1, c.size());
		assertEquals(doc1, c.iterator().next());
		
		verifyNoMoreInteractions(writer);
	}

	@Test
	public void testMarkTouchedCacheHit() {
		when(cache.markTouched(any(DocumentID.class), anyString())).thenReturn(
				true);

		assertTrue(io.markTouched(mock(DocumentID.class), "tag"));
	}

	@Test
	public void testMarkTouchedCacheMiss() {
		when(cache.markTouched(any(DocumentID.class), anyString())).thenReturn(
				false);
		when(writer.markTouched(any(DocumentID.class), anyString()))
				.thenReturn(true);
		
		when(writer.update(any(DatabaseDocument.class))).thenReturn(true);

		DatabaseDocument d = mock(DatabaseDocument.class);
		
		when(reader.getDocumentById(any(DocumentID.class))).thenReturn(d);
		
		assertTrue(io.markTouched(mock(DocumentID.class), "tag"));

		verify(d).removeFetchedBy(CachingDocumentNIO.CACHE_TAG);
		verify(d).setTouchedBy(eq("tag"), any(Date.class));
		verify(cache, times(1)).markTouched(any(DocumentID.class), anyString());
		verify(writer, times(1))
				.update(eq(d));
		verify(cache, never()).add(anyCollection());
	}

	@Test
	public void testMarkProcessed() {
		when(cache.getDocumentById(id1)).thenReturn(doc1);
		when(writer.markProcessed(doc1, "tag")).thenReturn(true);

		assertTrue(io.markProcessed(doc1, "tag"));

		verify(cache, times(1)).remove(id1);
		verify(writer, times(1)).markProcessed(doc1, "tag");

		verify(doc1, times(1)).putAll(doc1);
	}

	@Test
	public void testMarkDiscarded() {
		when(cache.getDocumentById(id1)).thenReturn(doc1);
		when(writer.markDiscarded(doc1, "tag")).thenReturn(true);

		assertTrue(io.markDiscarded(doc1, "tag"));

		verify(cache, times(1)).remove(id1);
		verify(writer, times(1)).markDiscarded(doc1, "tag");

		verify(doc1, times(1)).putAll(doc1);
	}

	@Test
	public void testMarkFailed() {
		when(cache.getDocumentById(id1)).thenReturn(doc1);
		when(writer.markFailed(doc1, "tag")).thenReturn(true);

		assertTrue(io.markFailed(doc1, "tag"));

		verify(cache, times(1)).remove(id1);
		verify(writer, times(1)).markFailed(doc1, "tag");

		verify(doc1, times(1)).putAll(doc1);
	}

	@Test
	public void testMarkPending() {
		when(cache.getDocumentById(id1)).thenReturn(doc1);
		when(writer.markPending(doc1, "tag")).thenReturn(true);

		assertTrue(io.markPending(doc1, "tag"));

		verify(cache, times(1)).remove(id1);
		verify(writer, times(1)).markPending(doc1, "tag");

		verify(doc1, times(1)).putAll(doc1);
	}

	@Test
	public void testInsert() {
		io.insert(doc1);

		verifyNoMoreInteractions(cache);
		verify(writer, times(1)).insert(any(DatabaseDocument.class));
	}

	@Test
	public void testUpdateInCache() {
		when(cache.getDocumentById(id1)).thenReturn(doc1);
		when(cache.update(doc1)).thenReturn(true);

		io.update(doc1);

		verify(cache, times(1)).update(doc1);
		verify(writer, never()).update(doc1);
	}

	@Test
	public void testDelete() {
		io.delete(doc1);

		verify(cache, times(1)).remove(id1);
		verify(writer, times(1)).delete(doc1);
	}

	@Test
	public void testDeleteDocumentFile() {
		io.deleteDocumentFile(doc1, "file");

		verify(writer, times(1)).deleteDocumentFile(doc1, "file");
		verifyNoMoreInteractions(cache);
	}

	@Test
	public void testDeleteAll() {
		io.deleteAll();

		verify(cache).removeAll();
		verify(writer).deleteAll();
	}

	@Test
	public void testWrite() throws Exception {
		io.write(mock(DocumentFile.class));

		verify(writer).write(any(DocumentFile.class));
		verifyNoMoreInteractions(cache);
	}

	@Test
	public void testPrepare() {
		io.prepare();

		verify(writer).prepare();
		verify(cache).prepare();
	}

	@Test
	public void testGetDocumentInCache() {
		when(cache.getDocument(q1)).thenReturn(doc1);

		assertEquals(doc1, io.getDocument(q1));

		verify(cache).getDocument(q1);
		verifyNoMoreInteractions(writer);
	}

	@Test
	public void testGetDocumentNotInCache() {
		when(cache.getDocument(q1)).thenReturn(null);
		when(reader.getDocument(q1)).thenReturn(doc1);

		assertEquals(doc1, io.getDocument(q1));

		verify(cache).add(doc1);
		verify(cache).getDocument(q1);
		verify(reader).getDocument(q1);
		verifyNoMoreInteractions(writer);
	}

	@Test
	public void testGetDocumentByIdDocumentIDInCache() {
		when(cache.getDocumentById(id1)).thenReturn(doc1);

		assertEquals(doc1, io.getDocumentById(id1));

		verify(cache).getDocumentById(id1);

		verifyNoMoreInteractions(reader);
		verifyNoMoreInteractions(writer);
	}

	@Test
	public void testGetDocumentByIdDocumentIDNotInCache() {
		when(cache.getDocumentById(id1)).thenReturn(null);
		when(reader.getDocumentById(id1)).thenReturn(doc1);
		when(reader.getDocumentById(id1, false)).thenReturn(doc1);

		assertEquals(doc1, io.getDocumentById(id1));

		verify(cache).getDocumentById(id1);
		try {
			verify(reader).getDocumentById(id1);
		} catch (Throwable t) {
			verify(reader).getDocumentById(id1, false);
		}
		verify(cache).add(doc1);
		verifyNoMoreInteractions(writer);
	}
	
	@Test
	public void testGetDocumentByIdDocumentIDNotInCacheOrReader() {
		when(cache.getDocumentById(id1)).thenReturn(null);
		when(reader.getDocumentById(id1)).thenReturn(null);
		when(reader.getDocumentById(id1, false)).thenReturn(null);

		assertNull(io.getDocumentById(id1));

		verify(cache).getDocumentById(id1);
		try {
			verify(reader).getDocumentById(id1);
		} catch (Throwable t) {
			verify(reader).getDocumentById(id1, false);
		}
		verify(cache, times(0)).add(any(DatabaseDocument.class));
		verify(cache, times(0)).add(any(Collection.class));
		verifyNoMoreInteractions(writer);
	}

	@Test
	public void testGetDocumentByIdNotInactive() {
		when(cache.getDocumentById(id1)).thenReturn(doc1);

		assertEquals(doc1, io.getDocumentById(id1, false));

		verify(cache, atLeastOnce()).getDocumentById(id1);

		verifyNoMoreInteractions(reader);
		verifyNoMoreInteractions(writer);
	}

	@Test
	public void testGetDocumentByIdInactive() {
		when(cache.getDocumentById(id1)).thenReturn(null);
		when(reader.getDocumentById(id1, true)).thenReturn(doc1);
		when(reader.getDocumentById(id1, false)).thenReturn(null);

		assertEquals(doc1, io.getDocumentById(id1, true));

		verify(cache).getDocumentById(id1);
		verify(cache, never()).add(doc1);

		verify(reader).getDocumentById(id1, true);
	}

	@Test
	public void testGetInactiveIterator() {
		io.getInactiveIterator();

		verifyNoMoreInteractions(cache, writer);
		verify(reader).getInactiveIterator();
	}

	@Test
	public void testGetInactiveIteratorWithQuery() {
		io.getInactiveIterator(q1);

		verifyNoMoreInteractions(cache, writer);
		verify(reader).getInactiveIterator(q1);
	}

	@Test
	public void testGetDocumentsInCache() {
		List list = Arrays.asList(new DatabaseDocument[] { doc1, doc2 });
		when(cache.getDocument(q1, 2)).thenReturn(list);

		list = io.getDocuments(q1, 2);
		assertTrue(list.contains(doc1));
		assertEquals(2, list.size());

		verify(cache).getDocument(q1, 2);
		verifyNoMoreInteractions(reader);
	}

	@Test
	public void testGetDocumentsNotInCache() {
		List list = Arrays.asList(new DatabaseDocument[] { doc1, doc2 });
		when(cache.getDocument(q1, 2)).thenReturn(
				new ArrayList<DatabaseDocument<TestType>>());
		when(reader.getDocuments(q1, 2)).thenReturn(list);

		list = io.getDocuments(q1, 2);
		assertEquals(2, list.size());
		assertTrue(list.contains(doc1));

		verify(cache).getDocument(q1, 2);
		verify(reader).getDocuments(q1, 2);
	}

	@Test
	public void testGetDocumentsNotInCacheOnly() {
		List list = Arrays.asList(new DatabaseDocument[] { doc1 });
		List list2 = Arrays.asList(new DatabaseDocument[] { doc1, doc2 });
		when(cache.getDocument(q1, 4)).thenReturn(list);
		when(reader.getDocuments(q1, 4)).thenReturn(list2);

		list = io.getDocuments(q1, 4);
		assertTrue(list.contains(doc1));
		assertEquals(1, list.size());

		verify(cache).getDocument(q1, 4);
		verifyNoMoreInteractions(reader);
	}

	@Test
	public void testGetDocumentsWithSkip() {
		List list = Arrays.asList(new DatabaseDocument[] { doc1, doc2 });
		
		when(reader.getDocuments(q1, 4, 1)).thenReturn(list);
		assertEquals(2, io.getDocuments(q1, 4, 1).size());
		
		verify(reader).getDocuments(q1, 4, 1);
	}

	@Test
	public void testGetDocumentFile() {
		when(reader.getDocumentFile(doc1, "file")).thenReturn(
				mock(DocumentFile.class));

		assertNotNull(io.getDocumentFile(doc1, "file"));

		verify(reader).getDocumentFile(doc1, "file");
		verifyNoMoreInteractions(cache);
	}

	@Test
	public void testGetDocumentFileNames() {
		when(reader.getDocumentFileNames(doc1)).thenReturn(
				Arrays.asList(new String[] { "file", "x" }));

		assertTrue(io.getDocumentFileNames(doc1).contains("x"));

		verify(reader).getDocumentFileNames(doc1);
		verifyNoMoreInteractions(cache);
	}

	@Test
	public void testGetActiveDatabaseSize() {
		when(reader.getActiveDatabaseSize()).thenReturn(10L);

		assertEquals(10, io.getActiveDatabaseSize());

		verify(reader).getActiveDatabaseSize();
		verifyNoMoreInteractions(cache);
	}

	@Test
	public void testGetInactiveDatabaseSize() {
		when(reader.getInactiveDatabaseSize()).thenReturn(20L);

		assertEquals(20, io.getInactiveDatabaseSize());

		verify(reader).getInactiveDatabaseSize();
		verifyNoMoreInteractions(cache);
	}

	@Test
	public void testToDocumentId() {
		when(reader.toDocumentId(any())).thenReturn(id1);

		assertEquals(id1, io.toDocumentId("xyz"));

		verify(reader).toDocumentId(any());
		verifyNoMoreInteractions(cache);
	}

	@Test
	public void testToDocumentIdFromJson() {
		when(reader.toDocumentIdFromJson(anyString())).thenReturn(id1);

		assertEquals(id1, io.toDocumentIdFromJson("xyz"));

		verify(reader).toDocumentIdFromJson(anyString());
		verifyNoMoreInteractions(cache);
	}

	@Test
	public void testConnect() throws Exception {
		io.connect();

		verify(connector).connect();
	}

	@Test
	public void testConvertLocalDocument() throws Exception {
		when(connector.convert(any(LocalDocument.class))).thenReturn(doc1);

		assertEquals(doc1, io.convert(new LocalDocument()));

		verify(connector).convert(any(LocalDocument.class));
	}

	@Test
	public void testConvertLocalQuery() {
		when(connector.convert(any(LocalQuery.class))).thenReturn(q1);

		assertEquals(q1, io.convert(new LocalQuery()));

		verify(connector).convert(any(LocalQuery.class));
	}

	@Test
	public void testCacheFlush() throws Exception {
		List list = Arrays.asList(new DatabaseDocument[] { doc1, doc2 });
		when(cache.removeAll()).thenReturn(list);
		
		io.flush();
		
		InOrder inOrder = inOrder(cache, writer, doc1);
		inOrder.verify(cache).removeAll();
		inOrder.verify(doc1).removeFetchedBy(CachingDocumentNIO.CACHE_TAG);
		inOrder.verify(writer).update(doc1);
		verify(doc2).removeFetchedBy(CachingDocumentNIO.CACHE_TAG);
		verify(writer).update(doc2);
	}
	
	@Test
	public void testCacheFlushTimeout() throws Exception {
		List list = Arrays.asList(new DatabaseDocument[] { doc1, doc2 });
		when(cache.removeStale(anyInt())).thenReturn(list);
		
		io.flush(1000);
		
		InOrder inOrder = inOrder(cache, writer, doc1);
		inOrder.verify(cache).removeStale(1000);
		inOrder.verify(doc1).removeFetchedBy(CachingDocumentNIO.CACHE_TAG);
		inOrder.verify(writer).update(doc1);
		verify(doc2).removeFetchedBy(CachingDocumentNIO.CACHE_TAG);
		verify(writer).update(doc2);
	}
	
	
}
