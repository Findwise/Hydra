package com.findwise.hydra.stage;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.JsonDeserializer;
import com.findwise.hydra.JsonException;
import com.findwise.hydra.SerializationUtils;

public class AbstractProcessStageMapper {
	Logger logger = LoggerFactory.getLogger(AbstractProcessStageMapper.class);

	public static final String ARG_NAME_STAGE_CLASS = "stageClass";

	public static AbstractProcessStage fromJsonString(String json) throws JsonException, ClassNotFoundException, InstantiationException, IllegalAccessException, InitFailedException, RequiredArgumentMissingException {
		Map<String, Object> properties = SerializationUtils.fromJson(json);
		return fromMap(properties);
	}

    @SuppressWarnings("unchecked")
	private static AbstractProcessStage fromMap(Map<String, Object> properties) throws RequiredArgumentMissingException, ClassNotFoundException, IllegalAccessException, InstantiationException, InitFailedException {
		String stageClass;
		if (properties.containsKey(ARG_NAME_STAGE_CLASS)) {
			stageClass = (String) properties.get(ARG_NAME_STAGE_CLASS);
		} else {
			throw new RequiredArgumentMissingException("No class specified in the '" + ARG_NAME_STAGE_CLASS + "' property.");
		}

		Class<? extends AbstractProcessStage> actualClass = (Class<? extends AbstractProcessStage>) Class
				.forName(stageClass);
		AbstractProcessStage stage = actualClass.newInstance();
		setParameters(stage, properties);
		stage.init();

		return stage;
	}

	/**
	 * Injects the parameters found in the map to any fields annotated with @Stage, whose names matches
	 * the keys in this map.
	 * @param map stage parameter map
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 */
	private static void setParameters(Object o, Map<String, Object> map) throws IllegalArgumentException, IllegalAccessException, RequiredArgumentMissingException {
		if (o.getClass().isAnnotationPresent(Stage.class)) {
			for(Field field : getParameterFields(o)) {
				Parameter fieldAnnotation = field.getAnnotation(Parameter.class);
				String parameterName = fieldAnnotation.name().isEmpty() ? field.getName() : fieldAnnotation.name();
				if (map.containsKey(parameterName)) {
					boolean prevAccessible = field.isAccessible();
					if (!prevAccessible) {
						field.setAccessible(true);
					}
					if(hasInterface(field.getType(), JsonDeserializer.class)) {
						try {
							JsonDeserializer jd = (JsonDeserializer) field.getType().newInstance();
							jd.fromJson(SerializationUtils.toJson(map.get(parameterName)));
							field.set(o, jd);
						} catch (InstantiationException e) {
							field.set(o, map.get(parameterName));
						} catch (JsonException e) {
							field.set(o, map.get(parameterName));
						}
					} else if(field.getType().isEnum() && !map.get(parameterName).getClass().isEnum()) {
						Object value = map.get(parameterName);
						try {
							if(value instanceof Integer) {
								field.set(o, field.getType().getEnumConstants()[(Integer)value]);
							} else if(value instanceof String) {
								field.set(o, field.getType().getDeclaredMethod("valueOf", String.class).invoke(null, value));
							}
						} catch (Exception e) {
							field.set(o, value);
						}
					}
					else {
						field.set(o, map.get(parameterName));
					}
					field.setAccessible(prevAccessible);
				} else if (field.getAnnotation(Parameter.class).required()) {
					throw new RequiredArgumentMissingException("Required parameter '" + parameterName + "' not configured");
				}
			}
		} else {
			throw new NoSuchElementException("No Stage-annotation found on the specified class " + o.getClass().getCanonicalName());
		}
	}

	private static boolean hasInterface(Class<?> c, Class<?> inf) {
		for(Class<?> x : c.getInterfaces()) {
			if(x.equals(inf)) {
				return true;
			}
		}
		return false;
	}

	public static List<Field> getParameterFields(Object o) {
		List<Field> list = new ArrayList<Field>();
		addParameterFields(list, o.getClass());
		return list;
	}

	private static void addParameterFields(List<Field> list, Class<?> startClass) {
		for (Field field : startClass.getDeclaredFields()) {
			if (field.isAnnotationPresent(Parameter.class)) {
				list.add(field);
			}
		}
		Class<?> superClass = startClass.getSuperclass();
		if(!superClass.equals(Object.class)) {
			addParameterFields(list, superClass);
		}
	}
}
