package com.findwise.hydra.mongodb;

import java.util.Arrays;
import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TaggingModelIT {
	MongoConnector mdc;

	private void createAndConnect() throws Exception {
		mdc = new MongoConnector(DatabaseConfigurationFactory.getDatabaseConfiguration("junit-TaggingModelTest"));
		
		mdc.waitForWrites(true);
		
		mdc.connect();
		
	}
	
	@Before
	public void setUp() throws Exception {
		createAndConnect();
		reset();
	}


	@After
	public void reset() throws Exception {
		mdc.getDB().dropDatabase();
		createAndConnect();
		
	}
	
	@AfterClass
	public static void tearDown() throws Exception {
		TaggingModelIT tmt = new TaggingModelIT();
		tmt.createAndConnect();
		tmt.mdc.getDB().dropDatabase();
	}
	
	@Test
	public void testDiff() throws Exception {
		int count = 1000;
		long[] currentMs = testCurrent(count);
		
		reset();
		
		//long[] otherMs = testArray(1000);

		System.out.println(Arrays.toString(currentMs));
	}
	
	
	public long[] testCurrent(int count) throws Exception {
		long[] ret = new long[4];
		ret[0] = insertCurrentDocuments(count);
		ret[1] = getCurrent(count, "tag");
		ret[2] = getCurrent(count, "tag2");
		ret[3] = getCurrent(count, "tag3");
		
		return ret;
	}
	
	public long getCurrent(int count, String tag) {
		long start = System.currentTimeMillis();
		System.out.println("Getting "+count+" documents for '"+tag+"'");
		MongoQuery mq = new MongoQuery();
		MongoDocumentIO reader = mdc.getDocumentReader();
		for(int i=0; i<count; i++) {
			if(i%(count/10) == 0) {
				System.out.print('.');
			}
			reader.getAndTag(mq, tag);
		}
		System.out.println();
		return System.currentTimeMillis()-start;
	}
	
	public long insertCurrentDocuments(int count) throws Exception {
		long start = System.currentTimeMillis();
		for(int i=0; i<count; i++) {
			MongoDocument d = new MongoDocument();
			d.putContentField(getRandomString(5), getRandomString(20));
			mdc.getDocumentWriter().insert(d);
		}
		return System.currentTimeMillis()-start;
	}

	
	private String getRandomString(int length) {
		char[] ca = new char[length];

		Random r = new Random(System.currentTimeMillis());

		for (int i = 0; i < length; i++) {
			ca[i] = (char) ('A' + r.nextInt(26));
		}

		return new String(ca);
	}
}
