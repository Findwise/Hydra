package com.findwise.hydra.admin.rest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.multipart.MultipartFile;

import com.findwise.hydra.Stage;
import com.findwise.hydra.admin.ConfigurationService;
import com.findwise.hydra.admin.documents.DocumentsService;

import com.findwise.hydra.admin.stages.StagesService;
import com.findwise.hydra.common.JsonException;


@Controller("/rest")
public class ConfigurationController {
	@Autowired
	private ConfigurationService<?> service;
	
	@Autowired
	private DocumentsService<?> documentService;

	@Autowired
	private StagesService<?> stagesService;
	
	public DocumentsService<?> getDocumentService() {
		return documentService;
	}

	public void setDocumentService(DocumentsService<?> documentService) {
		this.documentService = documentService;
	}
	public ConfigurationService<?> getService() {
		return service;
	}

	public void setService(ConfigurationService<?> service) {
		this.service = service;
	}

	
	@ResponseBody
	@RequestMapping(method=RequestMethod.GET, value="") 
	public Map<String, Object> getStats() {
		return service.getStats();
	}
	
	@ResponseBody
	@RequestMapping(method=RequestMethod.GET, value="/libraries")
	public Map<String, Object> getLibraries() {
		return service.getLibraries();
	}
	
	@ResponseBody
	@RequestMapping(method=RequestMethod.GET, value="/libraries/{id}")
	public Map<String, Object> getLibrary(@PathVariable String id) {
		return service.getLibrary(id);
	}
	
	@ResponseStatus(HttpStatus.ACCEPTED)
	@RequestMapping(method=RequestMethod.POST, value="/libraries/{id}")
	@ResponseBody
	public Map<String, Object> addLibrary(@PathVariable String id, @RequestParam MultipartFile file) throws IOException {
		service.addLibrary(id, file.getOriginalFilename(), file.getInputStream());
		return getLibrary(id);
	}
	
	@ResponseStatus(HttpStatus.ACCEPTED)
	@RequestMapping(method = RequestMethod.POST, value = "/libraries/{id}/stages/{stageName}")
	@ResponseBody
	public Map<String, Object> addStage(
			@PathVariable(value = "id") String libraryId,
			@PathVariable(value = "stageName") String stageName,
			@RequestBody String jsonConfig) throws JsonException, IOException {
		return stagesService.addStage(libraryId, stageName, jsonConfig);
	}


	@ResponseBody
	@RequestMapping(method = RequestMethod.GET, value = "/stages")
	public Map<String,List<Stage>> getStages() {
		return stagesService.getStages();
	}
	
	@ResponseBody
	@RequestMapping(method = RequestMethod.GET, value = "/stages/{stageName}")
	public Stage getStageInfo(@PathVariable(value = "stageName") String stageName) {
		return stagesService.getStageInfo(stageName);
	}

	@ResponseBody
	@RequestMapping(method = RequestMethod.GET, value = "/documents/count")
	public Map<String, Object> getDocumentCount(
			@RequestParam(required = false, defaultValue="{}", value = "q") String jsonQuery) {
		return documentService.getNumberOfDocuments(jsonQuery);
	}

	@ResponseBody
	@RequestMapping(method = RequestMethod.GET, value = "/documents")
	public Map<String, Object> getDocuments(
			@RequestParam(required = false, defaultValue = "{}", value = "q") String jsonQuery,
			@RequestParam(required = false, defaultValue = "10", value = "limit") int limit,
			@RequestParam(required = false, defaultValue = "0", value = "skip") int skip) {
		return documentService.getDocuments(jsonQuery, limit, skip);
	}

	@ResponseBody
	@RequestMapping(method = RequestMethod.POST, value = "/documents/edit")
	public Map<String, Object> editDocuments(
			@RequestParam(required = false, defaultValue = "{}", value = "q") String jsonQuery,
			@RequestParam(required = false, defaultValue = "1", value = "limit") int limit,
			@RequestBody String changes) {
		return documentService.updateDocuments(jsonQuery, limit, changes);
	}
	
}
