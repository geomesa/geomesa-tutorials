/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.geomesa.example.accumulo.auths;

import org.apache.accumulo.core.security.Authorizations;
import org.locationtech.geomesa.security.AuthorizationsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Implementation of AuthorizationsProvider that reads auths from ldap based on the spring security principal
 */
public class LdapAuthorizationsProvider implements AuthorizationsProvider {

    // name of the property file that will be used to configure this class
    public static final String PROPS_FILE = "geomesa-ldap.properties";

    // properties names for configuring this provider -
    // the ldap node to start the query from
    public static final String SEARCH_ROOT = "geomesa.ldap.search.root";

    // the query that will be applied to find the user's record
    public static final String SEARCH_FILTER = "geomesa.ldap.search.filter";

    // the ldap attribute that holds the comma-delimited authorizations for the user
    public static final String AUTHS_ATTRIBUTE = "geomesa.ldap.auths.attribute";

    private Properties environment;


    // the ldap node to start the query from
    String searchRoot;

    // the query that will be applied to find the user's record - the symbol {} will be replaced with the user's cn
    String searchFilter;

    // the ldap attribute that holds the comma-delimited authorizations for the user
    String authsAttribute;

    // Create the search controls for querying ldap
    SearchControls searchControls;

    private final Logger logger = LoggerFactory.getLogger(LdapAuthorizationsProvider.class);

    public void configure(Map<String, ? extends Serializable> params) {
        // load the properties from the props file on the classpath

        InputStream inputStream = getClass()
                .getClassLoader()
                .getResourceAsStream(PROPS_FILE);

        if (inputStream == null) {
            logger.error("Could not load resource {} - LDAP authorizations will not be enabled.",
                         PROPS_FILE);
            return;
        }

        environment = new Properties();
        try {
            environment.load(inputStream);
        } catch (IOException e) {
            logger.error("Error reading LDAP configuration from {}", PROPS_FILE, e);
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                logger.error("Error closing input stream", e);
            }
        }

        searchRoot = environment.getProperty(SEARCH_ROOT);
        searchFilter = environment.getProperty(SEARCH_FILTER);
        authsAttribute = environment.getProperty(AUTHS_ATTRIBUTE);
        searchControls = new SearchControls();
        // specify the fields we want returned - the auths in this case
        searchControls.setReturningAttributes(new String[] {"cn", authsAttribute});
        // limit the search to a subtree scope from the search root
        searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
    }

    public List<String> getAuthorizations() {
        // if there is an authenticated spring object, try to retrieve the auths from that, otherwise use empty auths
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        List<String> authList = new ArrayList<String>();
        authList.add(retrieveAuthorizationsFromPrincipal(authentication).toString());
        return authList;
    }

    /**
     * Retrieves the authorizations from ldap based on the spring principal
     *
     * @param auth
     *
     * @return
     */
    private Authorizations retrieveAuthorizationsFromPrincipal(Authentication auth) {
        if (auth == null) {
            return new Authorizations();
        }
        Object principal = auth.getPrincipal();

        // the principal should be a string containing the cn of the user
        if (!(principal instanceof String)) {
            logger
                    .debug("AuthorizationsProvider:: found unexpected principal type of {} - expected String",
                           principal.getClass());
            return new Authorizations();
        }

        String cn = (String) principal;

        // initialize the auth string - we will set it below if the ldap connection/query succeeds
        String authString = null;

        // the ldap context for querying ldap
        LdapContext context = null;

        try {
            // create the context - this may throw various exceptions for connections, authentication, etc {
            context = new InitialLdapContext(environment, null);

            // query the ldap server - note we replace the cn into the search filter
            NamingEnumeration<SearchResult> answer = context.search(searchRoot,
                                                                    searchFilter.replaceAll("\\{\\}",
                                                                                            cn),
                                                                    searchControls);
            if (answer.hasMoreElements()) {
                Attribute attribute = answer.next().getAttributes().get(authsAttribute);
                if (attribute != null) {
                    authString = (String) attribute.get();
                }
            }
            answer.close();
        } catch (Exception e) {
            logger.error("Error querying ldap", e);
        } finally {
            try {
                if (context != null) {
                    context.close();
                }
            } catch (Exception e) {
                logger.error("Error closing ldap connection", e);
            }
        }

        logger.debug("AuthorizationsProvider:: retrieved authorizations for user {} : {}",
                     cn,
                     authString);

        if (authString == null || authString.isEmpty()) {
            return new Authorizations();
        }

        return new Authorizations(authString.split(","));
    }
}
