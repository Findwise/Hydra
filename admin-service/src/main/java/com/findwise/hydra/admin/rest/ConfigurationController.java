package com.findwise.hydra.admin.rest;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.findwise.hydra.DatabaseException;
import com.google.gson.JsonParseException;
import com.mongodb.MongoException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.multipart.MultipartFile;

import com.findwise.hydra.Stage;
import com.findwise.hydra.StageGroup;
import com.findwise.hydra.admin.ConfigurationService;
import com.findwise.hydra.admin.documents.DocumentsService;

import com.findwise.hydra.admin.stages.StagesService;
import com.findwise.hydra.JsonException;

import javax.servlet.http.HttpServletRequest;


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
	public Map<String, Object> getStats() throws DatabaseException {
		return service.getStats();
	}
	
	@ResponseBody
	@RequestMapping(method=RequestMethod.GET, value="/libraries")
	public Map<String, Object> getLibraries() throws DatabaseException {
		return service.getLibraries();
	}
	
	@ResponseBody
	@RequestMapping(method=RequestMethod.GET, value="/libraries/{id}")
	public Map<String, Object> getLibrary(@PathVariable String id) throws DatabaseException {
		Map<String, Object> library = service.getLibrary(id);
		if (null != library) {
			return library;
		} else {
			throw new HttpResourceNotFoundException();
		}
	}
	
	@ResponseStatus(HttpStatus.ACCEPTED)
	@RequestMapping(method=RequestMethod.POST, value="/libraries/{id}")
	@ResponseBody
	public Map<String, Object> addLibrary(@PathVariable String id, @RequestParam MultipartFile file) throws DatabaseException, IOException {
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
		return stagesService.addStage(libraryId, null, stageName, jsonConfig);
	}

	@ResponseStatus(HttpStatus.ACCEPTED)
	@RequestMapping(method = RequestMethod.POST, value = "/libraries/{id}/stages/{groupName}/{stageName}")
	@ResponseBody
	public Map<String, Object> addStageToGroup(
			@PathVariable(value = "id") String libraryId,
			@PathVariable(value = "stageName") String stageName,
			@PathVariable(value = "groupName") String groupName,
			@RequestBody String jsonConfig) throws JsonException, IOException {
		return stagesService.addStage(libraryId, groupName, stageName, jsonConfig);
	}

	@ResponseBody
	@RequestMapping(method = RequestMethod.GET, value = "/stages")
	public Map<String,List<Stage>> getStages() {
		return stagesService.getStages();
	}
	
	@ResponseBody
	@RequestMapping(method = RequestMethod.GET, value = "/stages/{stageName}")
	public Stage getStageInfo(@PathVariable(value = "stageName") String stageName) {
		Stage stageInfo = stagesService.getStageInfo(stageName);
		if (null != stageInfo) {
			return stageInfo;
		} else {
			throw new HttpResourceNotFoundException();
		}
	}

	@ResponseBody
	@RequestMapping(method = RequestMethod.GET, value = "/stages/{stageName}/delete")
	public Map<String, Object> deleteStage(
			@PathVariable(value = "stageName") String stageName) throws IOException{
		return stagesService.deleteStage(stageName);
	}

	@ResponseBody
	@RequestMapping(method = RequestMethod.GET, value = "/stagegroups")
	public Map<String,List<StageGroup>> getStageGroups() {
		return stagesService.getStageGroups();
	}
	
	@ResponseBody
	@RequestMapping(method = RequestMethod.GET, value = "/stagegroups/{stageGroup}")
	public StageGroup getStageGroup(@PathVariable(value = "stageGroup") String stageGroup) {
		StageGroup stageGroupInfo = stagesService.getStageGroup(stageGroup);
		if (null != stageGroupInfo) {
			return stageGroupInfo;
		} else {
			throw new HttpResourceNotFoundException();
		}
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

        @ResponseBody
	@RequestMapping(method = RequestMethod.GET, value = "/documents/discard")
	public Map<String, Object> discardDocuments(
			@RequestParam(required = true, value = "q") String jsonQuery,
			@RequestParam(required = false, defaultValue = Integer.MAX_VALUE + "", value = "limit") int limit,
			@RequestParam(required = false, defaultValue = "0", value = "skip") int skip){
		return documentService.discardDocuments(jsonQuery, limit, skip);
	}
	
	@ResponseStatus(HttpStatus.ACCEPTED)
	@ResponseBody
	@RequestMapping(method = RequestMethod.POST, value = "/documents/new")
	public Map<String, Object> newDocument(
			@RequestParam(required = true, defaultValue = "ADD", value = "_action") String action,
			@RequestBody String content) {
		return documentService.putDocument(action, content);
	}

	@ResponseStatus(HttpStatus.BAD_REQUEST)
	@ResponseBody
	@ExceptionHandler(JsonException.class)
	public Map<String, Object> handleJsonError(Exception exception) {
		return getErrorMap(exception);
	}

	@ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
	@ResponseBody
	@ExceptionHandler({IOException.class, DatabaseException.class, MongoException.class})
	public Map<String, Object> handleIoError(Exception exception) {
		return getErrorMap(exception);
	}

	private Map<String, Object> getErrorMap(Exception exception) {
		Map<String, Object> ret = new HashMap<String, Object>();
		ret.put("message", exception.getMessage());
		ret.put("exception", exception.getClass());
		return ret;
	}
}
