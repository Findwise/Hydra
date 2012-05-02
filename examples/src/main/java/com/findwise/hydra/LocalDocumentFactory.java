package com.findwise.hydra;

import java.util.Random;

import org.apache.commons.lang.RandomStringUtils;

import com.findwise.hydra.common.Document.Action;
import com.findwise.hydra.local.LocalDocument;

public class LocalDocumentFactory {
	private static Random random = new Random();
	
	public static LocalDocument getRandomDocument(String ... wantedFields) {
		LocalDocument ld = new LocalDocument();
		ld.setAction(Action.ADD);
		
		for(String field : wantedFields) {
			ld.putContentField(field, getRandomValue());
		}
		
		return ld;
	}
	
	public static LocalDocument getRandomStringDocument(String ... wantedFields) {
		LocalDocument ld = new LocalDocument();
		ld.setAction(Action.ADD);
		
		for(String field : wantedFields) {
			ld.putContentField(field, getRandomStringValue());
		}
		
		return ld;
	}
	
	public static Object getRandomValue() {
		switch(random.nextInt(5)) {
		case 0:
			return random.nextInt();
		case 1:
			return random.nextBoolean();
		case 2:
			return random.nextDouble();
		case 3:
			return getRandomStringValue();
		default:
			String[] strings = new String[random.nextInt(5)];
			for(int i=0; i<strings.length; i++) {
				strings[i] = getRandomStringValue();
			}
			return strings;
		}
	}
	
	public static String getRandomStringValue() {
		return RandomStringUtils.randomAlphanumeric(1+random.nextInt(20));
	}
}
