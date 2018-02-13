/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.geomesa.example.transformations;

import org.apache.commons.cli.ParseException;
import org.geomesa.example.data.GDELTData;
import org.geomesa.example.data.TutorialData;
import org.geomesa.example.quickstart.GeoMesaQuickStart;
import org.geotools.data.DataAccessFactory.Param;
import org.geotools.data.Query;
import org.opengis.filter.Filter;

import java.util.ArrayList;
import java.util.List;

public abstract class GeoMesaQueryTutorial extends GeoMesaQuickStart {

    public GeoMesaQueryTutorial(String[] args, Param[] parameters) throws ParseException {
        super(args, parameters, new GDELTData(), true);
    }

    @Override
    public List<Query> getTestQueries(TutorialData data) {
        List<Query> queries = new ArrayList<>();

        // we'll use the same filter for each query
        Filter filter = data.getSubsetFilter();

        String typeName = data.getTypeName();

        // add some different queries - see each method for details
        queries.add(basicQuery(typeName, filter));
        queries.add(basicProjectionQuery(typeName, filter));
        queries.add(basicTransformationQuery(typeName, filter));
        queries.add(renamedTransformationQuery(typeName, filter));
        queries.add(mutliFieldTransformationQuery(typeName, filter));
        queries.add(geometricTransformationQuery(typeName, filter));

        return queries;
    }

    /**
     * Executes a basic bounding box query without any projections.
     *
     * @param typeName simple feature type name (schema)
     * @param filter query filter
     * @return query
     */
    public Query basicQuery(String typeName, Filter filter) {
        // use the 2-arg constructor for the query - this will not restrict the attributes returned
        return new Query(typeName, filter);
    }

    /**
     * Executes a query that restricts the attributes coming back.
     *
     * @param typeName simple feature type name (schema)
     * @param filter query filter
     * @return query
     */
    public Query basicProjectionQuery(String typeName, Filter filter) {
        // define the properties (attributes) that we want returned as a string array
        // each element of the array is a property name we want returned
        String[] properties = new String[] { "Actor1Name", "geom" };

        // create the query - we use the extended constructor to pass in our projection
        return new Query(typeName, filter, properties);
    }

    /**
     * Executes a query that transforms the results coming back to say 'hello' to each result.
     *
     * @param typeName simple feature type name (schema)
     * @param filter query filter
     * @return query
     */
    public Query basicTransformationQuery(String typeName, Filter filter) {
        // define the properties that we want returned

        // this also allows us to manipulate properties using various GeoTools transforms.
        // In this case, we are using a string concatenation to say 'hello' to our results. We
        // are overwriting the existing field with the results of the transform

        // the list of available transform functions is available here:
        // http://docs.geotools.org/latest/userguide/library/main/filter.html - scroll to 'Function List'

        String[] properties = new String[] { "Actor1Name=strConcat('hello ',Actor1Name)", "geom" };

        // create the query - we use the extended constructor to pass in our transform
        return new Query(typeName, filter, properties);
    }

    /**
     * Executes a query that returns a new dynamic field name created by transforming a field.
     *
     * @param typeName simple feature type name (schema)
     * @param filter query filter
     * @return query
     */
    public Query renamedTransformationQuery(String typeName, Filter filter) {
        // define the properties that we want returned

        // this also allows us to manipulate properties using various GeoTools transforms.
        // In this case, we are using a string concatenation to say 'hello' to our results. We are
        // storing the result of the transform in a new dynamic field, called 'derived'. We also
        // return the original attribute unchanged

        // the list of available transform functions is available here:
        // http://docs.geotools.org/latest/userguide/library/main/filter.html - scroll to 'Function List'

        String[] properties = new String[] { "Actor1Name", "derived=strConcat('hello ',Actor1Name)", "geom" };

        // create the query - we use the extended constructor to pass in our transform
        return new Query(typeName, filter, properties);
    }

    /**
     * Executes a query with a transformation on multiple fields.
     *
     * @param typeName simple feature type name (schema)
     * @param filter query filter
     * @return query
     */
    public Query mutliFieldTransformationQuery(String typeName, Filter filter) {
        // define the properties that we want returned

        // this also allows us to manipulate properties using various GeoTools transforms.
        // In this case, we are concatenating two different attributes

        // the list of available transform functions is available here:
        // http://docs.geotools.org/latest/userguide/library/main/filter.html - scroll to 'Function List'

        String[] properties = new String[] {"derived=strConcat(strConcat(Actor1Name,' - '),ActionGeo_FullName)", "geom" };

        // create the query - we use the extended constructor to pass in our transform
        return new Query(typeName, filter, properties);
    }

    /**
     * Creates a query that performs a geometric function transform on the result set
     *
     * @param typeName simple feature type name (schema)
     * @param filter query filter
     * @return query
     */
    public Query geometricTransformationQuery(String typeName, Filter filter) {
        // define the properties that we want returned

        // this also allows us to manipulate properties using various GeoTools transforms.
        // In this case, we are buffering the point to create a polygon. The transformed field gets
        // renamed to 'derived'

        // the list of available transform functions is available here:
        // http://docs.geotools.org/latest/userguide/library/main/filter.html - scroll to 'Function List'

        String[] properties = new String[] { "geom", "derived=buffer(geom, 2)"};

        // create the query - we use the extended constructor to pass in our transform
        return new Query(typeName, filter, properties);
    }
}