package com.example.geomesa.authorizations;

import org.apache.accumulo.core.security.Authorizations;
import org.locationtech.geomesa.security.AuthorizationsProvider;

import java.io.Serializable;
import java.util.Map;

/**
 * Implementation of auth provider that always returns empty auths
 */
public class EmptyAuthorizationsProvider
        implements AuthorizationsProvider {

    public Authorizations getAuthorizations() {
        return new Authorizations();
    }

    public void configure(Map<String, Serializable> params) {

    }
}
