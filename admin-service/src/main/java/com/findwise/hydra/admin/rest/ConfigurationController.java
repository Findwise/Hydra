package com.findwise.hydra.admin.rest;

import java.io.InputStream;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

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
	
	@ResponseStatus(value = HttpStatus.ACCEPTED)
	@RequestMapping(method=RequestMethod.POST, value="/library")
	public void addLibrary(@RequestParam String id, @RequestBody InputStream stream) {
		service.addLibrary(id, stream);
	}

	@ResponseBody
	@RequestMapping(method=RequestMethod.GET, value="") 
	public Map<String, Object> getStats() {
		return service.getStats();
	}
}
