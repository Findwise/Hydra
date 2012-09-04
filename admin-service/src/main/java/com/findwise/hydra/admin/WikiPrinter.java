package com.findwise.hydra.admin;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.reflections.Reflections;

import com.findwise.hydra.admin.StageScanner.ParameterMeta;
import com.findwise.hydra.stage.Stage;

public class WikiPrinter {
	public static void printWikiTable(List<Class<?>> classes) {
		System.out.println("^ Name ^ Package ^ Description ^ Parameters ^");
		for(Class<?> c : classes) {
			Stage s = c.getAnnotation(Stage.class);
			System.out.println("| "+c.getSimpleName() + " | " + c.getPackage().getName()+ " | "+s.description()+" | "+getParametersString(c)+" | ");
		}
	}

	public static String getParametersString(Class<?> c) {
		ArrayList<String> list = new ArrayList<String>();

		List<ParameterMeta> paramList = StageScanner.getParameters(c);
		for(ParameterMeta pt : paramList) {
			String desc = pt.getDescription().equals("") ? "" : " - "+pt.getDescription();
			list.add("**"+pt.getName()+"** ("+pt.getType()+") "+ desc);
		}
		
		if(list.size()==1) {
			return list.get(0);
		}
		
		return StringUtils.join(list, "\\\\ \\\\ ");
	}
	
	public static void main(String[] args) {
		StageScanner ss = new StageScanner();
		List<Class<?>> classes = ss.getClasses(new Reflections("com.findwise"));
		printWikiTable(classes);
	}
}
