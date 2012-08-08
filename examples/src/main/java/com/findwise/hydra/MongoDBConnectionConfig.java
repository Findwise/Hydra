package com.findwise.hydra;

import com.findwise.hydra.DatabaseConfiguration;
import com.findwise.hydra.mongodb.MongoConnector;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Names;

/**
 * Provides connection details to a mongodb instance. 
 * 
 * @author johan.sjoberg
 */
public class MongoDBConnectionConfig extends AbstractModule {

    private final String namespace;
    private final String connectionUrl;
    private final String username;
    private final String password;

    /**
     * Creates a new module with the given namespace and connection string. 
     * 
     * @param namespace mongodb namespace
     * @param connectionUrl mongodb connection url, e.g., "127.0.0.1"
     */
    public MongoDBConnectionConfig(String namespace, String connectionUrl) {
        this(namespace, connectionUrl, null, null);
    }

    /**
     * Creates a new module with with the given namespace, connection string, username and password
     * 
     * @param namespace mongodb namespace
     * @param connectionUrl mongodb connection url, e.g., "127.0.0.1"
     * @param username mongodb username
     * @param password mongodb password
     */
    public MongoDBConnectionConfig(String namespace, String connectionUrl, String username, String password) {
        this.namespace = namespace;
        this.connectionUrl = connectionUrl;
        this.username = username;
        this.password = password;
    }

    @Override
    protected void configure() {
        // guice wiring
        DatabaseConfiguration c = getConfiguration();
        bindConstant().annotatedWith(Names.named(DatabaseConnector.NAMESPACE_PARAM)).to(c.getNamespace());
        bindConstant().annotatedWith(Names.named(DatabaseConnector.DATABASE_URL_PARAM)).to(c.getDatabaseUrl());
		bindConstant().annotatedWith(Names.named(DatabaseConnector.DATABASE_USER)).to(c.getDatabaseUser());
		bindConstant().annotatedWith(Names.named(DatabaseConnector.DATABASE_PASSWORD)).to(c.getDatabasePassword());
        bind(DatabaseConnector.class).to(MongoConnector.class);
    }

    @Provides
    @Singleton
    protected DatabaseConfiguration getConfiguration() {
        return new DatabaseConfiguration() {

            @Override
            public String getNamespace() {
                return namespace;
            }

            @Override
            public String getDatabaseUrl() {
                return connectionUrl;
            }

            @Override
            public String getDatabaseUser() {
                return username;
            }

            @Override
            public String getDatabasePassword() {
                return password;
            }
        };
    }
}
