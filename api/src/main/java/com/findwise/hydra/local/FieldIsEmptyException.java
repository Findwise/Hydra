package com.findwise.hydra.local;

public class FieldIsEmptyException extends Exception {

	public FieldIsEmptyException() {
		super();
	}

	public FieldIsEmptyException(String message) {
		super(message);
	}

	public FieldIsEmptyException(String message, Throwable cause) {
		super(message, cause);
	}

	public FieldIsEmptyException(Throwable cause) {
		super(cause);
	}
}
