package com.findwise.hydra.admin.rest;

import com.findwise.hydra.admin.rest.jsonp.JsonpCallbackFilter;
import java.io.File;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.web.multipart.commons.CommonsMultipartResolver;

import com.findwise.hydra.admin.ConfigurationService;
import com.findwise.hydra.admin.documents.DocumentsService;
import com.findwise.hydra.admin.stages.StagesService;
import com.findwise.hydra.mongodb.MongoConnector;
import com.findwise.hydra.mongodb.MongoType;

@Configuration
@ComponentScan(basePackages = "com.findwise.hydra.admin.rest")
public class AppConfig {

	private static final String PROPERTIES_FILE = "admin-service.properties";
	private static final String CONFIG_DIR_PROPERTY = "hydra.admin.config.dir";

	@Autowired
	private DatabaseConfig databaseConfig;

	@Bean
	@Autowired
	public MongoConnector connector(DatabaseConfig config) {
		return new MongoConnector(config);
	}

	@Bean
	public DatabaseConfig databaseConfig() {
		return new DatabaseConfig();
	}

	@Bean(name = "multipartResolver")
	public CommonsMultipartResolver multipartResolver() {
		CommonsMultipartResolver cmr = new CommonsMultipartResolver();

		cmr.setMaxUploadSize(1024 * 1024 * 1024); // 1 Gigabyte...

		return cmr;
	}

        @Bean(name = "jsonpCallbackFilter")
        public static JsonpCallbackFilter jsonpCallback(){
            return new JsonpCallbackFilter();
        }

	@Bean
	public PropertyPlaceholderConfigurer properties() {
		String configurationDirectory = System.getProperty(CONFIG_DIR_PROPERTY);
		File propertiesFile = new File(configurationDirectory, PROPERTIES_FILE);
		final Resource[] resources = new Resource[] {
				new ClassPathResource(PROPERTIES_FILE),
				new FileSystemResource(propertiesFile) };
		PropertyPlaceholderConfigurer ppc = new PropertyPlaceholderConfigurer();
		ppc.setIgnoreUnresolvablePlaceholders(true);
		ppc.setIgnoreResourceNotFound(true);
		ppc.setLocations(resources);
		return ppc;
	}

	@Bean
	public ConfigurationService<MongoType> configurationService(
			MongoConnector connector) {
		return new ConfigurationService<MongoType>(connector);
	}

	@Bean
	public DocumentsService<MongoType> documentsService(MongoConnector connector) {
		return new DocumentsService<MongoType>(connector);

	}

	@Bean
	public StagesService<MongoType> stagesService(MongoConnector connector) {
		return new StagesService<MongoType>(connector);
	}

}