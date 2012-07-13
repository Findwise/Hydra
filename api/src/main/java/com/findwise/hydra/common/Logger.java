package com.findwise.hydra.common;


import java.io.PrintWriter;
import java.io.StringWriter;

import com.findwise.hydra.common.Logger.Level;

/**
 * This class is mirrors the Log4J error levels, providing an interface
 * for logging on stdout which will be redirected to Log4J by 
 * com.findwise.hydra.StreamLogger.
 *
 * Does not currently support multi-line log messages (each line is treated as a new message)
 *
 * @author joel.westberg
 */
public final class Logger {
	
	private Logger() {}

    public static enum Level { TRACE, DEBUG, INFO, WARN, ERROR, OFF }

    private static final String[] PREFIXES = {"TRACE ", "DEBUG ", "INFO ", "WARN ", "ERROR ", "OFF "};

    public static final String SWITCHMESSAGE = "$SWITCHLOGGER$ ";
    
    public static final String STACKTRACEMESSAGE = "$STACKTRACE$ ";

    private static Level internalLoggingLevel = Level.WARN;

	private static Level globalLevel = Level.TRACE;

    /**
     * Sets the name of the Log4J logger (equivalent to <code>new log4j.Logger(String clazz)</code>)
     *
     * NOTE: There is only ONE logger for the entire JVM.
     *
     * @param clazz
     */
    public static void setLogger(String clazz) {
        System.out.println(SWITCHMESSAGE + clazz);
    }

    /**
     * Logs without any appended level.
     * @param s
     */
    public static void log(String s) {
        System.out.println(s);
    }

    /**
     * Logs on the specified level
     * @param l
     * @param s
     */
    public static void log(Level l, String s) {
    	if(!logOnLevel(l)) {
    		return;
    	}
        System.out.println(PREFIXES[l.ordinal()]+s);
    }
    
    /**
     * Logs on the specified level
     * @param l
     * @param s
     */
    public static void log(Level l, String s, Throwable e) {
    	if(!logOnLevel(l)) {
    		return;
    	}
    	System.out.println(STACKTRACEMESSAGE);
    	log(l, s);
        StringWriter st = new StringWriter();
        e.printStackTrace(new PrintWriter(st));
    	System.out.println(st.toString());
    	System.out.println(STACKTRACEMESSAGE);
    }

    /**
     * Logs on the DEBUG level
     * @param s
     */
    public static void debug(String s) {
        log(Level.DEBUG, s);
    }
    
    /**
     * Logs on the DEBUG level
     * @param s
     * @param e
     */
    public static void debug(String s, Throwable e) {
    	log(Level.DEBUG, s, e);
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
     * @param e
     */
    public static void error(String s, Throwable e) {
        log(Level.ERROR, s, e);
    }

    /**
     * Logs on the INFO level
     * @param s
     */
    public static void info(String s) {
        log(Level.INFO, s);
    }
    
    /**
     * Logs on the INFO level
     * @param s
     * @param e
     */
    public static void info(String s, Throwable e) {
        log(Level.INFO, s, e);
    }

    /**
     * Logs on the OFF level
     * @param s
     */
    public static void off(String s) {
        log(Level.OFF, s);
    }

    /**
     * Logs on the OFF level
     * @param s
     * @param e
     */
    public static void off(String s, Throwable e) {
        log(Level.OFF, s, e);
    }
    
    /**
     * Logs on the TRACE level
     * @param s
     */
    public static void trace(String s) {
        log(Level.TRACE, s);
    }
    
    /**
     * Logs on the TRACE level
     * @param s
     * @param e
     */
    public static void trace(String s, Throwable e) {
        log(Level.TRACE, s, e);
    }

    /**
     * Logs on the WARN level
     * @param s
     */
    public static void warn(String s) {
        log(Level.WARN, s);
    }
    
    /**
     * Logs on the WARN level
     * @param s
     * @param e
     */
    public static void warn(String s, Throwable e) {
        log(Level.WARN, s, e);
    }

    /**
     * Gets the current level of messages above which the API itself will log.
     * @return
     */
    public static Level getInternalLoggingLevel() {
        return internalLoggingLevel;
    }

    /**
     * Set the level of logging for internal components. This defaults to WARN or above (i.e. WARN, ERROR, FATAL).
     * When developing inside the API, or if you are curious, set this to a lower level.
     */
    public static void setInternalLoggingLevel(Level internalLevel) {
        internalLoggingLevel = internalLevel;
    }
    
    /**
     * Sets the global logging level. 
     */
    public static void setGlobalLoggingLevel(Level loggingLevel) {
    	globalLevel  = loggingLevel;
    }
    
    public static Level getGlobalLoggingLevel() {
    	return globalLevel;
    }
    
    private static boolean logOnLevel(Level l) {
    	return l.ordinal()>=globalLevel.ordinal() && l!=Level.OFF;
    }
}
