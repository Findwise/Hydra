package com.findwise.hydra.json;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;

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
            return element.getAsString();
        } else if (element.isJsonArray()) {
            String[] array = jdc.deserialize(element, String[].class);
            return new ArrayList<String>(Arrays.asList(array));
        }
        throw new IllegalArgumentException("Illegal pipeline format, expecting either a primitive type or an array");
    }
}
