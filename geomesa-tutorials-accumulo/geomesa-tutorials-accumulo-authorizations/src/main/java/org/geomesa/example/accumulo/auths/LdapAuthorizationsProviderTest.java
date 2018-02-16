/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.geomesa.example.accumulo.auths;

import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

/**
 * Integration test for connecting to LDAP for authorizations
 */
public class LdapAuthorizationsProviderTest {

    public static void main(String[] args) {
        String user = null;
        if (args != null && args.length > 0) {
            user = args[0];
        }

        if (user == null || user.isEmpty()) {
            user = "rod";
        }

        // create the provider and initialize it with the 'configure' method
        LdapAuthorizationsProvider provider = new LdapAuthorizationsProvider();
        provider.configure(new HashMap<String, Serializable>());

        // set dummy authentication token corresponding to user 'rod'
        SecurityContextHolder.getContext().setAuthentication(new TestingAuthenticationToken(user, null));

        System.out.println("Checking auths from LDAP for user '" + user + "'");

        // get the authorizations - this will connect to ldap using the values in geomesa-ldap.properties
        List<String> auths = provider.getAuthorizations();

        System.out.println("Retrieved auths: " + auths);
    }
}
