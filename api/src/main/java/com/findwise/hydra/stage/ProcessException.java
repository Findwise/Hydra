package com.findwise.hydra.stage;

/**
 * @author karl.neyvaldt
 */
public class ProcessException extends Exception {

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
