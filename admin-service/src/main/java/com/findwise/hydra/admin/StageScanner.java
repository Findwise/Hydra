package com.findwise.hydra.admin;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.reflections.Reflections;

import com.findwise.hydra.stage.Parameter;
import com.findwise.hydra.stage.Stage;


/**
 * Scans for classes on classpath which are annotated with the @Stage parameter
 * @author joel.westberg
 *
 */
public class StageScanner {
	
	public List<Class<?>> getClasses(List<URL> jars) {
		ClassLoader cl = new URLClassLoader(jars.toArray(new URL[jars.size()]));
		
		Reflections reflections = new Reflections(jars, cl);
		return getClasses(reflections);
	}
	
	public List<Class<?>> getClasses(Reflections reflections) {
		List<Class<?>> classes = new ArrayList<Class<?>>();
		
		Set<Class<?>> cs = reflections.getTypesAnnotatedWith(Stage.class);
		Class<?>[] arr = cs.toArray(new Class<?>[cs.size()]);
		Arrays.sort(arr, new Comparator<Class<?>>() {
			public int compare(Class<?> o1, Class<?> o2) {
				return o1.getCanonicalName().compareTo(o2.getCanonicalName());
			}
		});
		for(Class<?> c : arr) {
			classes.add(c);
		}
		
		return classes;
	}
	
	public static class ParameterMeta {
		private String name;
		private String description;
		private String type;
		private boolean required;
		
		public ParameterMeta(String name, String description, String type, boolean required) {
			this.name = name;
			this.description = description;
			this.type = type;
			this.required = required;
		}
		
		public String getDescription() {
			return description;
		}
		public String getName() {
			return name;
		}
		public String getType() {
			return type;
		}
		
		public boolean isRequired() {
			return required;
		}
	}
	
	public static List<ParameterMeta> getParameters(Class<?> c) {
		ArrayList<ParameterMeta> list = new ArrayList<ParameterMeta>();
		
		addParameters(c, list);
		
		return list;
	}
	
	private static void addParameters(Class<?> c, ArrayList<ParameterMeta> list) {
		Class<?> superClass = c.getSuperclass();
		if(!superClass.equals(Object.class)) {
			addParameters(superClass, list);
		}
		for (Field field : c.getDeclaredFields()) {
			if (isAnnotationPresent(field.getAnnotations(), Parameter.class)) {
				list.add(getParameterMeta(field));
			}
		}
	}
	
	public static String getStageDescription(Class<?> clazz) {
		for(Annotation a : clazz.getAnnotations()) { 
			if(a.annotationType().getName().equals(Stage.class.getName())) {
				try {
					return (String) a.annotationType().getMethod("description").invoke(a);
				} catch (Exception e) {
					return null;
				}
			}
		}
		return null;
	}
	
	public static ParameterMeta getParameterMeta(Field field) {
		for(Annotation a : field.getAnnotations()) { 
			if(a.annotationType().getName().equals(Parameter.class.getName())) {
				try {
					String desc = (String) a.annotationType().getMethod("description").invoke(a);
					String name = (String) a.annotationType().getMethod("name").invoke(a);
					boolean required = (Boolean) a.annotationType().getMethod("required").invoke(a);
					
					if(name.equals("")) {
						name = field.getName();
					}
					return new ParameterMeta(name, desc, field.getType().getSimpleName(), required);
				} catch (Exception e) {
					return null;
				}
			}
		}
		return null;
	}
	
	public static boolean hasStageAnnotation(Class<?> clazz) {
		return isAnnotationPresent(clazz.getAnnotations(), Stage.class);
	}

	public static boolean isAnnotationPresent(Annotation[] annotations, Class<?> annotation) {
		for(Annotation a : annotations) {
 			if(a.annotationType().getName().equals(annotation.getName())) {
 				return true;
 			}
		}
		return false;
	}
}
