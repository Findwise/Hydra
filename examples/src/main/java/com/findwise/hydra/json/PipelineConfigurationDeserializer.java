package com.findwise.hydra.json;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Json deserializer for {@link PipelineConfiguration}. Allows stage configurations
 * of type primitive types, which are treated like strings, and of array type, which
 * are treated like an array of strings. 
 * 
 * @author johan.sjoberg
 */
public class PipelineConfigurationDeserializer implements JsonDeserializer<Object> {

    @Override
    public Object deserialize(JsonElement element, Type type, JsonDeserializationContext jdc) throws JsonParseException {
        if (element.isJsonPrimitive()) {
        	JsonPrimitive primitive = element.getAsJsonPrimitive();
        	if(primitive.isBoolean()) {
        		return primitive.getAsBoolean();
        	} else if(primitive.isNumber()) {
        		return primitive.getAsNumber();
        	} else {
        		return primitive.getAsString();
        	}
        } else if (element.isJsonArray()) {
            Object[] array = jdc.deserialize(element, Object[].class);
            return new ArrayList<Object>(Arrays.asList(array));
        } else if (element.isJsonObject()) {
        	Set<Entry<String,JsonElement>> entrySet = element.getAsJsonObject().entrySet();
        	Map<String, Object> map = new HashMap<String, Object>();
        	for (Entry<String, JsonElement> entry : entrySet) {
				map.put(entry.getKey(), jdc.deserialize(entry.getValue(), Object.class));
			}
        	return map;
        }
        throw new IllegalArgumentException("Illegal pipeline format, expecting either a primitive type or an array or an object");
    }
}
