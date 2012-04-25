package com.findwise.hydra.stage;

/**
 * Created by IntelliJ IDEA.
 * User:  karl.neyvaldt
 * Date:  5/9/11
 * Time:  4:36 PM
 * E-mail: karl.neyvaldt@findwise.com
 */
public class ProcessException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public ProcessException(String msg){
		super(msg);
	}
	
	public ProcessException() {
		super();
	    }
	
	public ProcessException(Throwable cause) {
		super(cause);
	}
	
    public ProcessException(String message, Throwable cause) {
        super(message, cause);
    }
}
