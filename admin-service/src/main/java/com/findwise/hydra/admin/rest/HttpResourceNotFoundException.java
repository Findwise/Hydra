package com.findwise.hydra.admin.rest;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Simple exception for returning HTTP_NOT_FOUND.
 * 
 * @author olof.nilsson
 *
 */
@ResponseStatus(value = HttpStatus.NOT_FOUND)
public class HttpResourceNotFoundException extends RuntimeException {

	private static final long serialVersionUID = 201303131633L;

}
