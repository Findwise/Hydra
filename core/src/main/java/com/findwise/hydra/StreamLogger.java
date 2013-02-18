package com.findwise.hydra;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;


/**
 * An OutputStream implementation that logs any data written to it using logback/slf4j.
 * 
 * @author joel.westberg
 */
public class StreamLogger extends OutputStream {
	private ByteArrayOutputStream os;
	
	/**
	 * A message written that starts with this string will switch which Log4J Logger is used.
	 */
	public static final String SWITCHMESSAGE = com.findwise.hydra.Logger.SWITCHMESSAGE;
	
	/**
	 * A message written at the beginning and end of a stacktrace printout.
	 */
	public static final String STACKTRACEMESSAGE = com.findwise.hydra.Logger.STACKTRACEMESSAGE;
	
	private Logger logger;
	
	private Level lastLevel;
	
	private boolean inStackTrace = false;
	
	
	
	public StreamLogger(String clazz) {
		logger = LoggerFactory.getLogger(clazz);
		os = new ByteArrayOutputStream();
	}
	
	private void log(String s) {
		int index = s.indexOf(' ');

		if (index == -1){
			log(Level.DEBUG, "UNLABLED OUTPUT : "+s);
		}
		else {
			lastLevel = Level.toLevel(s.substring(0, index), Level.OFF);

			if (lastLevel.equals(Level.OFF) && !s.toLowerCase().startsWith("off ")) {
				//Let's output this as DEBUG and mark it UNLABLED
				log(Level.DEBUG, "UNLABLED OUTPUT : "+s.substring(index+1));
			}
			else {
				log(lastLevel, s.substring(index+1));
			}
		}
	}
	
	private void log(Level l, String s) {
		if(l==Level.DEBUG) {
			logger.debug(s);
		} else if (l==Level.INFO) {
			logger.info(s);
		} else if (l==Level.WARN) {
			logger.warn(s);
		} else if (l==Level.ERROR) {
			logger.error(s);
		} else if (l==Level.TRACE) {
			logger.trace(s);
		}
	}
	
	@Override
	public void write(int b) throws IOException {
		if(b == (int)'\n') {
			String msg = os.toString();
			if(inStackTrace) {
				if(msg.endsWith(STACKTRACEMESSAGE)) {
					inStackTrace=false;
					msg=msg.substring(0, msg.length()-(STACKTRACEMESSAGE.length()+1));
				}
				else {
					os.write(b);
				}
			}
			else{
				if(msg.startsWith(STACKTRACEMESSAGE)) {
					os.reset();
					inStackTrace=true;
				}
			}
			if (!inStackTrace) {
				if (msg.startsWith(SWITCHMESSAGE)) {
					logger = LoggerFactory.getLogger(msg.substring(SWITCHMESSAGE.length()));
				} else {
					log(msg);
					os.reset();
				}
				os.reset();
			}
		} 
		else {
			os.write(b);
		}
	}
}
