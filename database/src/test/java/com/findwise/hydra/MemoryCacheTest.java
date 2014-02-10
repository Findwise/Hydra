package com.findwise.hydra;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class MemoryCacheTest {

	public static interface TestType extends DatabaseType {
	}

	List list;

	@Mock
	DatabaseDocument<TestType> doc1, doc2;

	@Mock
	DocumentID<TestType> id1, id2;
	
	@Mock
	DatabaseQuery<TestType> q1, q2, qAll;
	
	private MemoryCache<TestType> cache;
	
	@Before
	public void setUp() {
		cache = new MemoryCache<TestType>();
		
		when(doc1.getID()).thenReturn(id1);
		when(doc2.getID()).thenReturn(id2);

		when(doc1.copy()).thenReturn(doc1);
		when(doc2.copy()).thenReturn(doc2);
		
		when(doc1.matches(q1)).thenReturn(true);
		when(doc1.matches(qAll)).thenReturn(true);
		
		when(doc2.matches(q2)).thenReturn(true);
		when(doc2.matches(qAll)).thenReturn(true);
		
		list = Arrays.asList(new DatabaseDocument[] {doc1, doc2});
		
	}
	
	@Test
	public void testAddDatabaseDocument() {
		cache.add(doc1);
		
		assertEquals(doc1, cache.getDocumentById(id1));
	}
	
	@Test
	public void testAddNullDocumentIsNoOp() {
		cache.add( (DatabaseDocument<MemoryCacheTest.TestType>) null );
		assertEquals(0, cache.getSize());
	}

	@Test
	public void testAddCollectionOfDatabaseDocuments() {
		cache.add(list);

		assertEquals(doc1, cache.getDocumentById(id1));
		assertEquals(doc2, cache.getDocumentById(id2));
	}

	@Test
	public void testRemove() {
		cache.add(list);
		
		assertEquals(doc1, cache.remove(id1));
		
		assertNull(cache.getDocumentById(id1));
		assertNull(cache.remove(id1));
		assertEquals(doc2, cache.getDocumentById(id2));
	}

	@Test
	public void testRemoveAll() {
		cache.add(list);
		
		Collection<DatabaseDocument<TestType>> c = cache.removeAll();
		assertEquals(2, c.size());
		assertTrue(c.contains(doc1));
		assertTrue(c.contains(doc2));
		
		assertNull(cache.getDocumentById(id1));
		assertNull(cache.getDocumentById(id2));
		
		assertEquals(0, cache.removeAll().size());
	}

	@Test
	public void testGetDocument() {
		cache.add(list);
		
		assertNotNull(cache.getDocument());
	}

	@Test
	public void testGetDocumentWithQuery() {
		cache.add(list);
		
		assertEquals(doc1, cache.getDocument(q1));
		assertEquals(doc2, cache.getDocument(q2));
		assertNotNull(cache.getDocument(qAll));
	}

	@Test
	public void testGetDocumentWithQueryAndLimit() {
		cache.add(list);
		
		Collection<DatabaseDocument<TestType>> c = cache.getDocument(q1, 3);
		assertEquals(1, c.size());
		assertTrue(c.contains(doc1));
		
		c = cache.getDocument(qAll, 3);
		assertEquals(2, c.size());
		assertTrue(c.contains(doc1));
		assertTrue(c.contains(doc2));
		
		c = cache.getDocument(qAll, 1);
		assertEquals(1, c.size());
		assertTrue(list.contains(c.iterator().next()));
	}

	@Test
	public void testGetAndTag() {
		cache.add(list);
		
		assertEquals(doc1, cache.getAndTag(q1, "tag"));
		
		InOrder inOrder = inOrder(doc1, q1);
		inOrder.verify(q1).requireNotFetchedByStage("tag");
		inOrder.verify(doc1).setFetchedBy(eq("tag"), any(Date.class));
	}

	@Test
	public void testGetAndTagWithLimitNotFilled() {
		cache.add(list);
		
		Collection<DatabaseDocument<TestType>> c = cache.getAndTag(q1, 3, "tag");
		assertEquals(1, c.size());
		assertTrue(c.contains(doc1));
		
		InOrder inOrder = inOrder(doc1, doc2, q1, qAll);
		inOrder.verify(q1).requireNotFetchedByStage("tag");
		inOrder.verify(doc1).setFetchedBy(eq("tag"), any(Date.class));
	}
	
	@Test
	public void testGetAndTagWithLimitFilled() {
		cache.add(list);
		
		Collection<DatabaseDocument<TestType>> c;
		
		InOrder inOrder = inOrder(doc1, doc2, q1, qAll);
		
		c = cache.getAndTag(qAll, 3, "tag2");
		assertEquals(2, c.size());
		assertTrue(c.contains(doc1));
		assertTrue(c.contains(doc2));
		
		inOrder.verify(qAll).requireNotFetchedByStage("tag2");
		inOrder.verify(doc1).setFetchedBy(eq("tag2"), any(Date.class)); 
		verify(doc2).setFetchedBy(eq("tag2"), any(Date.class)); //Not necessarily after doc1...
	}

	@Test
	public void testUpdate() {
		cache.add(doc1);

		DatabaseDocument doc3 = mock(DatabaseDocument.class);
		when(doc3.getID()).thenReturn(id1);
		
		assertTrue(cache.update(doc3));
		verify(doc1).putAll(doc3);
		
		assertFalse(cache.update(doc2));
	}

	@Test
	public void testMarkTouched() {
		cache.add(doc1);
		
		assertTrue(cache.markTouched(id1, "tag"));
		verify(doc1).setTouchedBy(eq("tag"), any(Date.class));
		
		assertFalse(cache.markTouched(id2, "tag2"));
		verifyNoMoreInteractions(doc2);
	}

	@Test
	public void testGetSize() {
		assertEquals(0, cache.getSize());
		cache.add(list);
		assertEquals(2, cache.getSize());
		
		cache.remove(id1);
		assertEquals(1, cache.getSize());
		
		cache.remove(id1);
		assertEquals(1, cache.getSize());
		
	}

	@Test
	public void testRemoveStaleUntouched() throws Exception {
		cache.add(list);
		
		Thread.sleep(10);
		
		Collection<DatabaseDocument<TestType>> c = cache.removeStale(5);
		assertEquals(2, c.size());
		assertTrue(c.containsAll(list));
		assertEquals(0, cache.getSize());
		
		cache.add(doc1);
		Thread.sleep(50);
		cache.add(doc2);
		
		c = cache.removeStale(10);
		assertEquals(1, c.size());
		assertEquals(1, cache.getSize());
		assertEquals(doc1, c.iterator().next());
	}


	@Test
	public void testRemoveStaleFetched() throws Exception {
		cache.add(list);
		
		Thread.sleep(50);
		cache.getAndTag(q1, "tag");
		
		Collection<DatabaseDocument<TestType>> c = cache.removeStale(40);
		assertEquals(1, c.size());
		assertTrue(c.contains(doc2));
		assertEquals(1, cache.getSize());
		
		Thread.sleep(5);
		c = cache.removeStale(1);
		assertEquals(1, c.size());
		assertTrue(c.contains(doc1));
		assertEquals(0, cache.getSize());
	}
	


	@Test
	public void testRemoveStaleTouched() throws Exception {
		cache.add(list);
		
		Thread.sleep(50);
		cache.markTouched(id1, "tag");
		
		Collection<DatabaseDocument<TestType>> c = cache.removeStale(40);
		assertEquals(1, c.size());
		assertTrue(c.contains(doc2));
		assertEquals(1, cache.getSize());
		
		Thread.sleep(5);
		c = cache.removeStale(1);
		assertEquals(1, c.size());
		assertTrue(c.contains(doc1));
		assertEquals(0, cache.getSize());
	}
}
