package com.findwise.hydra;

import com.findwise.hydra.common.Document;

public interface DatabaseDocument<T extends DatabaseType> extends Document {
	Object putMetadataField(String key, Object value);
}
