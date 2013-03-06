package com.findwise.hydra.memorydb;

import java.util.Random;

import com.findwise.hydra.Document.Action;

public class TestTools {
	private static Random r = new Random(System.currentTimeMillis());

	public static MemoryDocument getRandomDocument() {
		MemoryDocument d = new MemoryDocument();
		for(int i=0; i<r.nextInt(10); i++) {
			if(r.nextBoolean()) {
				d.putContentField(TestTools.getRandomString(r.nextInt(10)), TestTools.getRandomString(r.nextInt(10)));
			} else {
				d.putContentField(TestTools.getRandomString(r.nextInt(10)), r.nextDouble()*r.nextInt(100));
			}
		}
		d.setAction(Action.ADD);
		
		return d;
	}

	public static String getRandomString(int length) {
		char[] ca = new char[length];
	
		for (int i = 0; i < length; i++) {
			ca[i] = (char) ('A' + r.nextInt(26));
		}
	
		return new String(ca);
	}

}
