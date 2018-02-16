/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.geomesa.example.cassandra;

import org.apache.commons.cli.ParseException;
import org.geomesa.example.data.GDELTData;
import org.geomesa.example.quickstart.GeoMesaQuickStart;
import org.locationtech.geomesa.cassandra.data.CassandraDataStoreFactory;

public class CassandraQuickStart extends GeoMesaQuickStart {

    // uses gdelt data
    public CassandraQuickStart(String[] args) throws ParseException {
        super(args, new CassandraDataStoreFactory().getParametersInfo(), new GDELTData());
    }

    public static void main(String[] args) {
        try {
            new CassandraQuickStart(args).run();
        } catch (ParseException e) {
            System.exit(1);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(2);
        }
        System.exit(0);
    }
}
