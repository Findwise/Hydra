package com.findwise.hydra.local;

public class IncorrectFieldTypeException extends Exception {

	public IncorrectFieldTypeException() {
		super();
	}

	public IncorrectFieldTypeException(String message) {
		super(message);
	}

	public IncorrectFieldTypeException(String message, Throwable cause) {
		super(message, cause);
	}

	public IncorrectFieldTypeException(Throwable cause) {
		super(cause);
	}
}
