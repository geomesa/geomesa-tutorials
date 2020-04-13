/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.geomesa.example.accumulo.auths;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.geomesa.example.data.GDELTData;
import org.geomesa.example.data.TutorialData;
import org.geomesa.example.quickstart.GeoMesaQuickStart;
import org.geotools.data.DataAccessFactory.Param;
import org.geotools.data.DataStore;
import org.geotools.data.Query;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams;
import org.locationtech.geomesa.security.AuthorizationsProvider;
import org.locationtech.geomesa.security.DefaultAuthorizationsProvider;
import org.locationtech.geomesa.security.SecurityUtils;
import org.opengis.feature.simple.SimpleFeature;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AuthorizationsTutorial extends GeoMesaQuickStart {

    // use gdelt data - overwrite the type name so we don't interfere with the regular quick start
    private static final TutorialData data = new GDELTData() {
        @Override
        public String getTypeName() {
            return "gdelt-secure";
        }
    };

    private DataStore unauthorizedDatastore = null;
    private String visibilities = null;

    public AuthorizationsTutorial(String[] args)  throws ParseException {
        super(args, new AccumuloDataStoreFactory().getParametersInfo(), data);
    }

    @Override
    public Options createOptions(Param[] parameters) {
        Options options = super.createOptions(parameters);
        // make visibilities and auths required
        Option vis = Option.builder(null)
                           .longOpt("visibilities")
                           .argName("visibilities")
                           .hasArg()
                           .desc("Visibilities to set on each feature")
                           .required(true)
                           .build();
        options.addOption(vis);
        Param authorizations = AccumuloDataStoreParams.AuthsParam();
        Option auths = Option.builder(null)
                           .longOpt(authorizations.getName())
                           .argName(authorizations.getName())
                           .hasArg()
                           .desc(authorizations.getDescription().toString())
                           .required(true)
                           .build();
        options.addOption(auths);
        return options;
    }

    @Override
    public void initializeFromOptions(CommandLine command) {
        super.initializeFromOptions(command);
        this.visibilities = command.getOptionValue("visibilities");
    }

    @Override
    public DataStore createDataStore(Map<String, String> params) throws IOException {
        // get an instance of the data store that uses our authorizations provider,
        // that always returns empty auths
        System.setProperty(AuthorizationsProvider.AUTH_PROVIDER_SYS_PROPERTY,
                           EmptyAuthorizationsProvider.class.getName());
        unauthorizedDatastore = super.createDataStore(params);

        // get an instance of the data store that uses the default authorizations provider,
        // which will use whatever auths the connector has available
        System.setProperty(AuthorizationsProvider.AUTH_PROVIDER_SYS_PROPERTY,
                           DefaultAuthorizationsProvider.class.getName());
        return super.createDataStore(params);
    }

    @Override
    public List<SimpleFeature> getTestFeatures(TutorialData data) {
        List<SimpleFeature> features = super.getTestFeatures(data);
        for (SimpleFeature feature : features) {
            feature.getUserData().put(SecurityUtils.FEATURE_VISIBILITY, visibilities);
        }
        return features;
    }

    @Override
    public List<Query> getTestQueries(TutorialData data) {
        // we'll use the same filter for each query
        return Collections.singletonList(new Query(data.getTypeName(), data.getSubsetFilter()));
    }

    @Override
    public void queryFeatures(DataStore datastore, List<Query> queries) throws IOException {
        // execute the query, with and without visibilities
        System.out.println("Executing query with AUTHORIZED data store: auths are '"
                           + ((AccumuloDataStore) datastore).config().authProvider().getAuthorizations() + "'");
        super.queryFeatures(datastore, queries);

        System.out.println("Executing query with UNAUTHORIZED data store: auths are '"
                           + ((AccumuloDataStore) unauthorizedDatastore).config().authProvider().getAuthorizations() + "'");
        super.queryFeatures(unauthorizedDatastore, queries);
    }

    @Override
    public void cleanup(DataStore datastore, String typeName, boolean cleanup) {
        if (unauthorizedDatastore != null) {
            unauthorizedDatastore.dispose();
        }
        super.cleanup(datastore, typeName, cleanup);
    }

    public static void main(String[] args) {
        try {
            new AuthorizationsTutorial(args).run();
        } catch (ParseException e) {
            System.exit(1);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(2);
        }
        System.exit(0);
    }
}