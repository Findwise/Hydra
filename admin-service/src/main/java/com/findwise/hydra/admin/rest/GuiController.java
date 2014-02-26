package com.findwise.hydra.admin.rest;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller("/gui")
public class GuiController {

	@RequestMapping(method = RequestMethod.GET, value = "/gui")
	public String gui() {
		return "/gui/index.html";
	}
}
