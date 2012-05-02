package com.findwise.hydra.stage.rules;

import com.findwise.hydra.common.Document;

/**
 * @author joel.westberg
 */
public interface OutputRule {
	boolean verify(Document d);
}
