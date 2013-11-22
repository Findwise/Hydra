package com.findwise.hydra;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.commons.codec.binary.Base64;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * Convenience methods for handling serialization and deserialization of 
 * objects to and from Json, using Gson. 
 * @author joel.westberg
 *
 */
public final class SerializationUtils {
	private static Logger logger = LoggerFactory.getLogger(SerializationUtils.class);
    private static Logger internalLogger = LoggerFactory.getLogger("internal");

    private static SimpleDateFormat getDateFormat() {
		return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
	}

	private static SimpleDateFormat getLegacyDateFormat() {
		return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
	}
	
	private static Gson gson = getGson();
	
	private static Gson getGson() {
		GsonBuilder gsonBuilder = new GsonBuilder();
		gsonBuilder.registerTypeAdapter(InputStream.class,
				new com.google.gson.JsonSerializer<InputStream>() {
					public JsonElement serialize(InputStream src, Type typeOfSrc, JsonSerializationContext context) {
						ByteArrayOutputStream baos = new ByteArrayOutputStream();
						try {
							for (int b; (b = src.read()) >= 0;) {
								baos.write(b);
							}
						} catch (IOException e) {
							logger.error("Caught IOException in Stream serialization", e);
						}
						return new JsonPrimitive(new String(Base64.encodeBase64(baos.toByteArray())));
					}
				});
		
		gsonBuilder.registerTypeAdapter(Date.class,
				new com.google.gson.JsonSerializer<Date>() {
					@Override
					public JsonElement serialize(Date src, Type typeOfSrc,
							JsonSerializationContext context) {
						return src == null ? null : new JsonPrimitive(getDateFormat().format(src));
					}
				});
		gsonBuilder.registerTypeAdapter(DocumentID.class,
				new com.google.gson.JsonSerializer<DocumentID<?>>() {
					@Override
					public JsonElement serialize(DocumentID<?> src, Type typeOfSrc,
							JsonSerializationContext context) {
						if(src == null) {
							return null;
						}
						String json = src.toJSON();
						if(json.startsWith("\"") && json.endsWith("\"")) {
							json = json.substring(1, json.length()-1);
						}
						return new JsonPrimitive(json);
					}
				});
		
		return gsonBuilder.serializeNulls().create();
	}
	
	private SerializationUtils() {}
	
	/**
	 * Method for Deserializing any Json message into a Map. 
	 * Should the Json message not be a map, it will be wrapped in a map, corresponding to
	 * the JSON: <code>{ "" : &lt;original_json&gt; }</code>
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> fromJson(String json) throws JsonException {
		try {
			Object o = toObject(json);
			if(o instanceof Map) {
				return (Map<String, Object>) o;
			}
			else {
				HashMap<String, Object> x = new HashMap<String, Object>();
				x.put("", o);
				return x;
			}
		}
		catch(JsonParseException e) {
			throw new JsonException(e);
		}
	}
	
	public static Object toObject(String json) throws JsonException {
		GsonBuilder gsonBuilder = new GsonBuilder();
		gsonBuilder.serializeNulls();
		gsonBuilder.registerTypeAdapter(Object.class, new NaturalGsonDeserializer());
		Gson gson = gsonBuilder.create();
		return gson.fromJson(json, Object.class);
	}
	
	/**
	 * Serializes any object to Json. 
	 * @param o
	 * @return
	 */
	public static String toJson(Object o) {
		while(true) {
			try {
				return gson.toJson(o);
			} catch(ConcurrentModificationException e) {
				logger.warn("A ConcurrentModificationException was caught during serialization. Trying again!");
			}
		}
	}
	
	/**
	 * Method for verifying what class the passed Object will get if serialized and then deserialized via this class.
	 */
	public static Class<?> getResultingClass(Object o) {
		return getResultingClass(toJson(o));
	}
	
	/**
	 * Functionally the same as calling SerializationUtils.toObject(<pre>json</pre>).getClass()
	 */
	public static Class<?> getResultingClass(String json) {
		try {
			return toObject(json).getClass();
		} catch (JsonException e) {
			return null;
		}
	}
	
	/**
	 * Private class needed to induce Gson to serialize standard "String" : Object relations in JSON string to DBObjects.
	 * In a more general case, DBObject could be a HashMap instead (see the equivalent parsing in hydra-api Document class)
	 */
	private static class NaturalGsonDeserializer implements JsonDeserializer<Object> {
		
		public Object deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) {
			try {
				if (json.isJsonNull()) {
					return null;
				} else if (json.isJsonPrimitive()) {
					return handlePrimitive(json.getAsJsonPrimitive());
				} else if (json.isJsonArray()) {
					return handleArray(json.getAsJsonArray(), context);
				} else {
					return handleObject(json.getAsJsonObject(), context);
				}
			} catch (Exception e) {
				internalLogger.error("An exception was caught during deserialization", e);
				return null;
			}
		}

		private Object handlePrimitive(JsonPrimitive json) {
			if (json.isBoolean()) {
				return json.getAsBoolean();
			} else if (json.isString()) {
				try {
					return getDateFormat().parse(json.getAsString());
				} catch(ParseException e) {
					try {
						return getLegacyDateFormat().parse(json.getAsString());
					} catch (ParseException e2) {
						return json.getAsString();
					}
				}
			} else {
				BigDecimal bigDec = json.getAsBigDecimal();
				
				try {
					bigDec.toBigIntegerExact();
					
					try {
						return bigDec.intValueExact();
					} catch (ArithmeticException e) {
						return bigDec.longValue();
					}
				} 
				catch (ArithmeticException e) {
				}
				return bigDec.doubleValue();
			}
		}

		private Object handleArray(JsonArray json, JsonDeserializationContext context) {
			List<Object> array = new ArrayList<Object>();
			for (int i = 0; i < json.size(); i++) {
				array.add(context.deserialize(json.get(i), Object.class));
			}
			return array;
		}

		private Object handleObject(JsonObject json, JsonDeserializationContext context) {
			try {
				HashMap<String, Object> map = new HashMap<String, Object>();
				for (Map.Entry<String, JsonElement> entry : json.entrySet()) {
					map.put(entry.getKey(), context.deserialize(entry.getValue(), Object.class));
				}
				return map;
			} catch(Exception e) {
				return null;
			}
		}
	}
}
