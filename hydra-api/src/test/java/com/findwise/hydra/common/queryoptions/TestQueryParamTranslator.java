package com.findwise.hydra.common.queryoptions;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.findwise.hydra.local.LocalQuery;
import com.findwise.hydra.local.QueryParamTranslator;
import com.findwise.hydra.local.StaticQueryParamTranslator;

public class TestQueryParamTranslator {

	@Before
	public void setUp() throws Exception {

	}

	@After
	public void tearDown() {

	}

	private Map<String, Boolean> generateMap(int n) {
		Map<String, Boolean> ret = new HashMap<String, Boolean>();
		Random r = new Random();
		for (int i = 0; i < n; i++) {
			ret.put("data" + i, r.nextBoolean());
		}
		return ret;
	}

	@Test
	public void testTranslateContents() {

		QueryParamTranslator qpt = new StaticQueryParamTranslator();

		LocalQuery q = new LocalQuery();
		q.getContentsExists().putAll(generateMap(100));
		assertEquals(q.getContentsExists(),
				qpt.createQueryFromString(qpt.extractQueryOptions(q))
						.getContentsExists());
	}

	@Test
	public void testTranslateTouched(){
		QueryParamTranslator qpt = new StaticQueryParamTranslator();
		LocalQuery q = new LocalQuery();
		q.getTouched().putAll(generateMap(100));
		assertEquals(q.getTouched(), qpt.createQueryFromString(qpt.extractQueryOptions(q)).getTouched());
	}
	
	@Test
	public void testTranslateMulti(){
		QueryParamTranslator qpt = new StaticQueryParamTranslator();
		LocalQuery q =new LocalQuery();
		
		q.getContentsExists().putAll(generateMap(100));
		q.getTouched().putAll(generateMap(200));
		
		LocalQuery res = qpt.createQueryFromString(qpt.extractQueryOptions(q));
		
		assertEquals(q.getTouched(), res.getTouched());
		assertEquals(q.getContentsExists(), res.getContentsExists());
		
	}
}
