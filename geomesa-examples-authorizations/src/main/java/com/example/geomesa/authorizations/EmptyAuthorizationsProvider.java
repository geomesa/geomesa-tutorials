/***********************************************************************
 * Copyright (c) 2014-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

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
