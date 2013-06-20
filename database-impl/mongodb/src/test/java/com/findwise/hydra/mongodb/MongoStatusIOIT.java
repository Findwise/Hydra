package com.findwise.hydra.mongodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Rule;
import org.junit.Test;

public class MongoStatusIOIT {
	
	@Rule
	public MongoDatabaseResource mongoResource = new MongoDatabaseResource(getClass());
	
	@Test
	public void testSaveInsert() {
		MongoStatusIO io = new MongoStatusIO(mongoResource.getDatabase());
		
		if(io.hasStatus() && io.getStatus().isPrepared()) {
			fail("Not set up properly");
		}
		
		MongoPipelineStatus mps = new MongoPipelineStatus();
		mps.setPrepared(true);
		io.save(mps);
		
		if(!io.getStatus().isPrepared()) {
			fail("Did not insert properly");
		}
		
	}
	
	@Test
	public void testCounts() {
		MongoStatusIO io = new MongoStatusIO(mongoResource.getDatabase());
		MongoPipelineStatus mps = new MongoPipelineStatus();
		
		if(mps.getFailedCount()!=0) {
			fail("Failed was non-zero from the start");
		}
		if(mps.getProcessedCount()!=0) {
			fail("Processed was non-zero from the start");
		}
		if(mps.getDiscardedCount()!=0) {
			fail("Discarded was non-zero from the start");
		}
		
		mps.setFailedCount(1);
		mps.setDiscardedCount(2);
		mps.setProcessedCount(3);
		
		io.save(mps);
		
		mps = io.getStatus();

		assertEquals(1, mps.getFailedCount());
		assertEquals(2, mps.getDiscardedCount());
		assertEquals(3, mps.getProcessedCount());
	}
	
	@Test
	public void testIncrement() {
		MongoStatusIO io = new MongoStatusIO(mongoResource.getDatabase());
		MongoPipelineStatus mps = new MongoPipelineStatus();
		io.save(mps);
		
		assertEquals(mps.getDiscardedCount(), 0);
		assertEquals(mps.getFailedCount(), 0);
		assertEquals(mps.getProcessedCount(), 0);
		
		io.increment(1, 3, Integer.MAX_VALUE);
		
		mps = io.getStatus();
		
		assertEquals(1, mps.getProcessedCount());
		assertEquals(3, mps.getFailedCount());
		assertEquals(Integer.MAX_VALUE, mps.getDiscardedCount());
	}
}
