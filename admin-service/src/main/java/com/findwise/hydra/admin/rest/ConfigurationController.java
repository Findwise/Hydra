package com.findwise.hydra.admin.rest;

import java.io.IOException;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.multipart.MultipartFile;

import com.findwise.hydra.admin.ConfigurationService;

@Controller("/rest")
public class ConfigurationController {
	@Autowired
	private ConfigurationService<?> service;
	
	public ConfigurationService<?> getService() {
		return service;
	}

	public void setService(ConfigurationService<?> service) {
		this.service = service;
	}

	@ResponseBody
	@RequestMapping(method=RequestMethod.GET, value="/library")
	public Map<String, Object> getLibraries() {
		return service.getLibraries();
	}
	
	@ResponseBody
	@RequestMapping(method=RequestMethod.GET, value="/library/{id}")
	public Map<String, Object> getLibrary(@PathVariable String id) {
		return service.getLibrary(id);
	}
	
	@ResponseStatus(value = HttpStatus.ACCEPTED)
	@RequestMapping(method=RequestMethod.POST, value="/library/{id}")
	@ResponseBody
	public Map<String, Object> addLibrary(@PathVariable String id, @RequestParam MultipartFile file) throws IOException {
		service.addLibrary(id, file.getOriginalFilename(), file.getInputStream());
		return getLibrary(id);
	}

	@ResponseBody
	@RequestMapping(method=RequestMethod.GET, value="") 
	public Map<String, Object> getStats() {
		return service.getStats();
	}
}
