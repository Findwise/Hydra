package com.findwise.hydra.local;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.findwise.hydra.common.Document.Action;

public class LocalQueryTest {

	@Test
	public void testFromJson() throws Exception {
		LocalQuery lq = new LocalQuery();
		
		lq.requireContentFieldEquals("x", 1);
		lq.requireContentFieldExists("x");
		lq.requireContentFieldNotExists("y");
		lq.requireTouchedByStage("s");
		lq.requireNotTouchedByStage("s2");
		lq.requireAction(Action.DELETE);
		
		LocalQuery lq2 = new LocalQuery(lq.toJson());
		if(!lq2.getContentsEquals().get("x").equals(1)) {
			fail("Equals was lost in serialization");
		}
		if(!lq2.getContentsExists().get("x")) {
			fail("Exists 'true' was lost in serialization");
		}
		if(lq2.getContentsExists().get("y")) {
			fail("Exists 'false' was lost in serialization");
		}
		if(!lq2.getTouched().get("s")) {
			fail("Touched 'true' was lost in serialization");
		}
		if(lq2.getTouched().get("s2")) {
			fail("Touched 'false' was lost in serialization");
		}
		if(lq2.getAction()!=Action.DELETE) {
			fail("Action was lost in serialization");
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testEqualsMapSerialization() throws Exception {
		LocalQuery lq, lq2;
		lq = new LocalQuery();
		lq.requireContentFieldEquals("x", "1");
		lq2 = new LocalQuery(lq.toJson());
		if(!lq2.getContentsEquals().get("x").equals("1")) {
			fail("Lost string");
		}

		lq = new LocalQuery();
		lq.requireContentFieldEquals("x", 1);
		lq2 = new LocalQuery(lq.toJson());
		if(!lq2.getContentsEquals().get("x").equals(1)) {
			fail("Lost int");
		}
		
		lq = new LocalQuery();
		lq.requireContentFieldEquals("x", 1.1);
		lq2 = new LocalQuery(lq.toJson());
		if(!lq2.getContentsEquals().get("x").equals(1.1)) {
			fail("Lost float");
		}
		
		lq = new LocalQuery();
		lq.requireContentFieldEquals("x", new int[]{1, 2, 3});
		lq2 = new LocalQuery(lq.toJson());
		if(((Integer)((List<Object>)lq2.getContentsEquals().get("x")).get(0))!=1) {
			fail("Lost array of int");
		}
		
		lq = new LocalQuery();
		lq.requireContentFieldEquals("x", new String[]{"1", "2", "3"});
		lq2 = new LocalQuery(lq.toJson());
		if(!((String)((List<Object>)lq2.getContentsEquals().get("x")).get(0)).equals("1")) {
			fail("Lost array of String");
		}
		
		lq = new LocalQuery();
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("key", "value");
		lq.requireContentFieldEquals("x", map);
		lq2 = new LocalQuery(lq.toJson());
		Map<String, Object> m = (Map<String, Object>)lq2.getContentsEquals().get("x");
		if(!m.containsKey("key") || !m.get("key").equals("value")) {
			fail("Map failed to deserialize properly");
		}
		
		lq = new LocalQuery();
		List<String> list = new ArrayList<String>();
		list.add("1");
		list.add("2");
		
		lq.requireContentFieldEquals("x", list);
		lq2 = new LocalQuery(lq.toJson());
		if(!((String)((List<Object>)lq2.getContentsEquals().get("x")).get(0)).equals("1")) {
			fail("Lost list of String");
		}
	}

}
