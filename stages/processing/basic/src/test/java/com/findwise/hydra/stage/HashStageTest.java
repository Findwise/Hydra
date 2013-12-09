package com.findwise.hydra.stage;

import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

import com.findwise.hydra.local.LocalDocument;


public class HashStageTest {
	
	

	@Test
	public void testMd5() throws Exception {
		HashStage hs = new HashStage();
		Map<String, String> map = new HashMap<String, String>();
		map.put("from", "to");
		hs.setMap(map);
		hs.init();
		
		testStringMD5(hs, RandomStringUtils.randomAscii(10000));
		testStringMD5(hs, RandomStringUtils.randomAlphanumeric(10000));
		
		for(int i=0;i<1000; i++) {
			testStringMD5(hs, RandomStringUtils.random(100));
		}
	}
	
	private void testStringMD5(HashStage hs, String s) throws ProcessException {
		LocalDocument ld = new LocalDocument();
		ld.putContentField("from", s);
		hs.processField(ld, "from", "to");
		
		if(!ld.getContentField("to").equals(DigestUtils.md5Hex(ld.getContentField("from").toString()))) {
			fail("Output not the same as DigestUtils.md5Hex for s='"+s+"'");
		}
	}
	
	@Test
	public void testSha256() throws Exception {
		HashStage hs = new HashStage();
		hs.setAlgorithm("SHA-256");
		Map<String, String> map = new HashMap<String, String>();
		map.put("from", "to");
		hs.setMap(map);
		hs.init();
		
		testStringSha256(hs, RandomStringUtils.randomAscii(10000));
		testStringSha256(hs, RandomStringUtils.randomAlphanumeric(10000));
		
		for(int i=0;i<1000; i++) {
			testStringSha256(hs, RandomStringUtils.random(100));
		}
	}
	
	private void testStringSha256(HashStage hs, String s) throws ProcessException {
		LocalDocument ld = new LocalDocument();
		ld.putContentField("from", s);
		hs.processField(ld, "from", "to");
		
		if(!ld.getContentField("to").equals(DigestUtils.sha256Hex(ld.getContentField("from").toString()))) {
			fail("Output not the same as DigestUtils.md5Hex for s='"+s+"'");
		}
	}
}
