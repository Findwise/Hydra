package com.findwise.hydra.admin.rest;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

@Configuration
@EnableWebMvc
public class WebConfig extends WebMvcConfigurerAdapter {

	@Override
	public void addResourceHandlers(ResourceHandlerRegistry registry) {
		registry.addResourceHandler("/gui/**").addResourceLocations("/WEB-INF/static/");
                  registry.addResourceHandler("/webjars/**").addResourceLocations("/webjars/");
		super.addResourceHandlers(registry);
	}
}
