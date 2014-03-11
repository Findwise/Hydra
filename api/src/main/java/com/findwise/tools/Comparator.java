package com.findwise.tools;

import java.lang.reflect.Method;
import java.math.BigDecimal;


/**
 * Utility class for comparing any two objects for equality.
 * 
 * @author joel.westberg
 */
public final class Comparator {
	private Comparator() {}
	
	/**
	 * Nullsafe method for checking equality of the values of two Number objects 
	 */
	public static boolean equals(Number o, Number p) {
		if(o==null && p==null) {
			return true;
		}
		if(o==null ^ p == null) {
			return false;
		}
		
		if(o.equals(p)) {
			return true;
		}
		
		return comparisonEquals(o, p);
	}
	
	/**
	 * Not nullsafe. Converts to double or long value, comparing these.
	 */
	private static boolean comparisonEquals(Number o, Number p) {
		if(isDecimal(o)) {
			if(isDecimal(p)) {
				return o.doubleValue() == p.doubleValue();
			}
			else {
				return o.doubleValue() == p.longValue();
			}
		} else {
			if(isDecimal(p)) {
				return o.longValue() == p.doubleValue();
			}
			else {
				return o.longValue() == p.longValue();
			}
		}
	}
	
	/**
	 * @return true if o is an instance of Double, Float or BigDecimal
	 */
	public static boolean isDecimal(Number o) {
		return o instanceof Double || o instanceof Float || o instanceof BigDecimal;
	}
	
	/**
	 * @return true if o is an instance of Byte, Integer, Long, Short, BigInteger, AtomicLong or AtomicInteger.
	 */
	public static boolean isInteger(Number o) {
		return !isDecimal(o);
	}
	
	/**
	 * True if o is Character or is a 1-length char array, or CharSequence.
	 */
	public static boolean isCharacter(Object o) {
		try {
			toCharacter(o);
		}
		catch(ClassCastException e) {
			return false;
		}
		
		return true;
	}
	
	/**
	 * Throws ClassCastException if the passed object cannot be converted to a
	 * Char. isCharacter() method can confirm if this is the case or not.
	 */
	public static Character toCharacter(Object o) {
		if (o instanceof Character) {
			return (Character) o;
		}

		if (o instanceof CharSequence && ((CharSequence) o).length() == 1) {
			return ((CharSequence) o).charAt(0);
		}

		if (o instanceof char[] && ((char[]) o).length == 1) {
			return ((char[]) o)[0];
		}

		if (o instanceof Character[] && ((Character[]) o).length == 1) {
			return ((Character[]) o)[0];
		}

		throw new ClassCastException("Cannot cast " + o.getClass().getName() + " to " + Character.class);
	}
	
	/**
	 * Nullsafe method for checking equality of the values of any two objects
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static boolean equals(Object o, Object p) {
		
		if(o==null && p == null) {
			return true;
		}
		else if(o == null || p == null) {
			return false;
		}
		else if(o.equals(p)) {
			return true;
		}
		
		Object o2 = o;
		Object p2 = p;
		
		if(isCharacter(o)) {
			o2 = (int)(char)toCharacter(o);
		}
		if(isCharacter(p)) {
			p2 = (int)(char)toCharacter(p);
		}
		
		try {
			return equals((Number) o2, (Number) p2);
		}
		catch(Exception e) {
			//Not Numbers
		}
		
		try {
			return ((Comparable) o2).compareTo(p2)==0;
		} catch(Exception e) {
			// Not comparable
		}
		
		return false;
	}
	
	/**
	 * This method will take a Class object and perform duck-typing on the other two
	 * supplied objects. If object o and o2 all have the getters of class C, these will be called and compared for equality.
	 * 
	 * @param c
	 * @param o
	 * @param o2
	 * @return true if o and o2 have the getters of c and their values are equal
	 */
	@SuppressWarnings("rawtypes")
	public static boolean getterEqual(Class c, Object o, Object o2) {
		Method[] methods = c.getMethods();
		for(Method m : methods) {
			if(isGetter(m)) {
				try {
					Object oRes = m.invoke(o);
					Object o2Res = m.invoke(o2);
					if(!oRes.equals(o2Res)) {
						return false;
					}
				}
				catch(Exception e) {
					return false;
				}
			}
		}
		
		return true;
	}
	
	private static boolean isGetter(Method m) {
		String name = m.getName();
		if(m.getParameterTypes().length!=0) {
			return false;
		}
		
		if(name.startsWith("get") && name.length()>3 && Character.isUpperCase(name.charAt(3))) {
			return true;
		}
		
		if(m.getReturnType().equals(Boolean.class) && name.startsWith("is") && name.length()>2 && Character.isUpperCase(name.charAt(2))) {
			return true;
		}
		
		return false;
	}
}
