package com.findwise.hydra.admin.rest;

import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import com.findwise.hydra.DatabaseConfiguration;
import com.findwise.hydra.admin.ConfigurationService;
import com.findwise.hydra.mongodb.MongoConnector;
import com.findwise.hydra.mongodb.MongoType;

@Configuration
@ComponentScan(basePackages = "com.findwise.hydra.admin.rest")
public class AppConfig {

	@Bean
	public static PropertyPlaceholderConfigurer properties() {
		PropertyPlaceholderConfigurer ppc = new PropertyPlaceholderConfigurer();
		final Resource[] resources = new ClassPathResource[] { };
		ppc.setLocations(resources);
		ppc.setIgnoreUnresolvablePlaceholders(true);
		return ppc;
	}

	@Bean
	public static ConfigurationService<MongoType> service() {
		return new ConfigurationService<MongoType>(new MongoConnector(
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
						return "admin";
					}

					public String getDatabaseUrl() {
						return "localhost";
					}

					public String getDatabasePassword() {
						return "changeme";
					}
				}));
	}

}