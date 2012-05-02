package com.findwise.tools;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Helper class to get Message Digests in a lightweight fashion. Primarily useful because it lets you
 * get a hex string of the digests instead of the raw bytes.
 * 
 * @author joel.westberg
 */
public class Hasher {
	private MessageDigest md; 
	
	private static final char[] DIGIT_CHARS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
	
	/**
	 * @param algorithm - See documentation for java.security.MessageDigest 
	 * @throws NoSuchAlgorithmException
	 */
	public Hasher(String algorithm) throws NoSuchAlgorithmException {
		md = MessageDigest.getInstance(algorithm);
	}
	
	public byte[] hashBytes(String content) {
		return md.digest(content.getBytes(Charset.forName("UTF-8")));
	}
	
	public MessageDigest getMessageDigest() {
		return md;
	}

	/**
	 * See
	 * http://svn.apache.org/viewvc/commons/proper/codec/trunk/src/main/java
	 * /org/apache/commons/codec/binary/Hex.java?view=markup
	 */
	public char[] hashChars(String content) {
		byte[] data = hashBytes(content);
		
		int l = data.length;
		char[] out = new char[l << 1];
		// two characters form the hex value.
		for (int i = 0, j = 0; i < l; i++) {
			out[j++] = DIGIT_CHARS[(0xF0 & data[i]) >>> 4];
			out[j++] = DIGIT_CHARS[0x0F & data[i]];
		}
		
		return out;
	}
	
	public String hashString(String content) {
		return new String(hashChars(content));
	}
}
