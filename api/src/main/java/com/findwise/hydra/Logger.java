package com.findwise.hydra;


import org.slf4j.LoggerFactory;

/**
 * Deprecated Logger implementation.
 *
 * @author joel.westberg
 */
@Deprecated
public final class Logger {
    public static org.slf4j.Logger logger = LoggerFactory.getLogger(Logger.class);
	private Logger() {}

    public static enum Level { TRACE, DEBUG, INFO, WARN, ERROR, OFF }

    /**
     * Sets the name of the Log4J logger (equivalent to <code>new log4j.Logger(String clazz)</code>)
     *
     * NOTE: There is only ONE logger for the entire JVM.
     *
     * @param clazz
     */
    synchronized public static void setLogger(String clazz) {
        logger = LoggerFactory.getLogger(clazz);
    }

    /**
     * Logs without any appended level.
     * @param s
     */
    public static void log(String s) {
        logger.info(s);
    }

    /**
     * Logs on the specified level
     * @param l
     * @param s
     */
    public static void log(Level l, String s) {
        switch(l) {
            case ERROR:
                logger.error(s);
                break;
            case WARN:
                logger.warn(s);
                break;
            case INFO:
                logger.info(s);
                break;
            case DEBUG:
                logger.debug(s);
                break;
            case TRACE:
                logger.trace(s);
                break;
            case OFF:
                // There is no OFF level in slf4j API as far as I can tell. I am guessing that these
                // shouldn't be logged at all? TODO: Confused...
                break;
        }
    }
    
    /**
     * Logs on the specified level
     * @param l
     * @param s
     */
    public static void log(Level l, String s, Throwable e) {
        switch(l) {
            case ERROR:
                logger.error(s, e);
                break;
            case WARN:
                logger.warn(s, e);
                break;
            case INFO:
                logger.info(s, e);
                break;
            case DEBUG:
                logger.debug(s, e);
                break;
            case TRACE:
                logger.trace(s, e);
                break;
            case OFF:
                // There is no OFF level in slf4j API as far as I can tell. I am guessing that these
                // shouldn't be logged at all? TODO: Confused...
                break;
        }
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
}
