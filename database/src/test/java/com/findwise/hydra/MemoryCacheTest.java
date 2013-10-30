package com.findwise.hydra;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

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

	/**
	 * Test case for competing for a critical section in {@link MemoryCache#getAndTag(DatabaseQuery, String...)}
	 *
	 * One thread (t1) will enter the section while the other (t2) is blocked until t1 terminates
	 * */
	@Test(timeout=10000) // async test case, timeout needed just in case ;)
	public void concurrentGetAndTagTest() throws Exception
	{
		final Semaphore semaphore = new Semaphore(0);
		final Semaphore t1Semaphore = new Semaphore(0);
		final ArrayList<DatabaseDocument<TestType>> results = new ArrayList<DatabaseDocument<TestType>>();
		final Thread t1 = new Thread() {
			@Override
			public void run() {
				DatabaseDocument<TestType> d1 = cache.getAndTag(q1, "foo");
				results.add(d1);
			}
		};
		final Thread t2 = new Thread() {
			@Override
			public void run() {
				DatabaseDocument<TestType> d2 = cache.getAndTag(q1, "bar");
				results.add(d2);
			}
		};

		Map<DocumentID<TestType>, DatabaseDocument<TestType>> map = new HashMap<DocumentID<TestType>, DatabaseDocument<TestType>>();
		map.put(id1, doc1);
		Map<DocumentID<TestType>, Long> lastTouched = mock(Map.class);
		when(lastTouched.containsKey(anyObject())).then(new Answer<Boolean>() {
			@Override
			public Boolean answer(InvocationOnMock invocation) throws Throwable {
				// this is a good place to intercept the first thread and block to trigger a race condition
				if (Thread.currentThread().equals(t1)) {
					try {
						semaphore.release();
						assertTrue(t1Semaphore.tryAcquire(1, 10000, TimeUnit.MILLISECONDS));
					} catch(InterruptedException e) {

						fail("Got interrupted while running test"); // NOTE: the fail is cannot be dispatched to JUnit test thread, but will result will be null!
					}
				}

				return true;
			}
		});

		cache.setUnitTestMode(map, lastTouched);

		t1.start();
		assertTrue(semaphore.tryAcquire(1, 10000, TimeUnit.MILLISECONDS));
		// t1 has been started and is now in the middle of cache#freshen, starting t2 will eventually trigger the race condition!

		t2.start();

		// if NOT synchronized, t2 should have finished after a short while.
		assertTrue(waitForThreadState(t2, Thread.State.BLOCKED, 10000));

		// t2 has been blocked - this is what we wanted. let t1 finish and and join the threads
		t1Semaphore.release();
		t1.join();
		assertEquals(Thread.State.TERMINATED, t1.getState());

		t2.join();
		assertEquals(Thread.State.TERMINATED, t2.getState());

		assertEquals("in thread-unsafe env, this would be 2", 1, cache.getSize());
		assertEquals("result from t1", doc1, results.get(0));

		// This will indirectly check that both threads have finished gracefully, since there
		// is a problem with dispatching assertionFailures across threads
		assertEquals("a thread was gracefully terminated", 2, results.size());
		// in a "real" environment, this would be null, since the "query.matches" would return false the second time!
		assertEquals("result from t2", doc1, results.get(1));
	}

	/** Polls the thread state */
	private boolean waitForThreadState(Thread t, Thread.State state, long timeout)
	{
		long now = System.currentTimeMillis();
		while (now + timeout > System.currentTimeMillis())
		{
			if (t.getState().equals(state)) {
				return true;
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				/*ignore*/
			}
		}

		return false;
	}
}
