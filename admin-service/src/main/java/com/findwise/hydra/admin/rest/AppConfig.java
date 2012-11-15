package com.findwise.hydra.admin.rest;

import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.web.multipart.commons.CommonsMultipartResolver;

import com.findwise.hydra.DatabaseConfiguration;
import com.findwise.hydra.admin.ConfigurationService;
import com.findwise.hydra.admin.documents.DocumentsService;
import com.findwise.hydra.admin.stages.StagesService;
import com.findwise.hydra.mongodb.MongoConnector;
import com.findwise.hydra.mongodb.MongoType;

@Configuration
@ComponentScan(basePackages = "com.findwise.hydra.admin.rest")
public class AppConfig {

	private static MongoConnector connector = new MongoConnector(
			new DatabaseConfiguration() {

				public int getOldMaxSize() {
					return 100;
				}

				public int getOldMaxCount() {
					return 10000;
				}

				public String getNamespace() {
					return "pipeline";
				}

				public String getDatabaseUser() {
					return "";
				}

				public String getDatabaseUrl() {
					return "localhost";
				}

				public String getDatabasePassword() {
					return "";
				}
			});

	@Bean(name = "multipartResolver")
	public static CommonsMultipartResolver multipartResolver() {
		CommonsMultipartResolver cmr = new CommonsMultipartResolver();

		cmr.setMaxUploadSize(1024 * 1024 * 1024); // 1 Gigabyte...

		return cmr;
	}

	@Bean
	public static PropertyPlaceholderConfigurer properties() {
		PropertyPlaceholderConfigurer ppc = new PropertyPlaceholderConfigurer();
		final Resource[] resources = new ClassPathResource[] {};
		ppc.setLocations(resources);
		ppc.setIgnoreUnresolvablePlaceholders(true);
		return ppc;
	}

	@Bean
	public static ConfigurationService<MongoType> configurationService() {
		return new ConfigurationService<MongoType>(connector);
	}

	@Bean
	public static DocumentsService<MongoType> documentsService() {
		return new DocumentsService<MongoType>(connector);

	}
	
	@Bean
	public static StagesService<MongoType> stagesService() {
		return new StagesService<MongoType>(connector);
	}

}