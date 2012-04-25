package com.findwise.hydra.common;

import com.findwise.hydra.common.Logger.Level;

public final class InternalLogger {
	private InternalLogger() {}
	
	/**
	 * Logs on the DEBUG level
	 * @param s
	 */
	public static void debug(String s) {
		log(Level.DEBUG, s);
	}
	
	/**
	 * Logs on the ERROR level
	 * @param s
	 */
	public static void error(String s) {
		log(Level.ERROR, s);
	}
	
	/**
	 * Logs on the ERROR level
	 * @param s
	 */
	public static void error(String s, Throwable e) {
		if(isAllowed(Level.ERROR)) {
			Logger.error(s, e);
		}
	}
	
	/**
	 * Logs on the INFO level
	 * @param s
	 */
	public static void info(String s) {
		log(Level.INFO, s);
	}
	
	/**
	 * Logs on the OFF level
	 * @param s
	 */
	public static void off(String s) {
		log(Level.OFF, s);
	}	
	
	/**
	 * Logs on the TRACE level
	 * @param s
	 */
	public static void trace(String s) {
		log(Level.TRACE, s);
	}	
	
	/**
	 * Logs on the WARN level
	 * @param s
	 */
	public static void warn(String s) {
		log(Level.WARN, s);
	}
	
	/**
	 * Logs on the specified level, assuming the specified level 
	 * is greater or equal to Logger.getInternalLoggingLevel()
	 * @param l
	 * @param s
	 */
	public static void log(Level l, String s) {
		//First checks if logging on this level is allowed
		if(isAllowed(l)) {
			Logger.log(l, s);
		}
	}
	
	/**
	 * @return true if logging is allowed on the specified level <code>l</code>.
	 */
	public static boolean isAllowed(Level l) {
		return l.compareTo(Logger.getInternalLoggingLevel())>=0;
	}
}
