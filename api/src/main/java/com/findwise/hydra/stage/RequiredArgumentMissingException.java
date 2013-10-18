package com.findwise.hydra.stage;

public class RequiredArgumentMissingException extends Exception {

	private static final long serialVersionUID = 1L;

	public RequiredArgumentMissingException(String msg){
		super(msg);
	}
	
	public RequiredArgumentMissingException() {
		super();
	    }
	
	public RequiredArgumentMissingException(Throwable cause) {
		super(cause);
	}
	
	public RequiredArgumentMissingException(String message, Throwable cause) {
		super(message, cause);
	}
}
