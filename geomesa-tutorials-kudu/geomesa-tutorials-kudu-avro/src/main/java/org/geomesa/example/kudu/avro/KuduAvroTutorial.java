/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.geomesa.example.kudu.avro;

import org.apache.commons.cli.ParseException;
import org.geomesa.example.avro.GeoMesaAvroTutorial;
import org.locationtech.geomesa.kudu.data.KuduDataStoreFactory;

public class KuduAvroTutorial extends GeoMesaAvroTutorial {

    public KuduAvroTutorial(String[] args) throws ParseException {
        super(args, new KuduDataStoreFactory().getParametersInfo());
    }

    public static void main(String[] args) {
        try {
            new KuduAvroTutorial(args).run();
        } catch (ParseException e) {
            System.exit(1);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(2);
        }
        System.exit(0);
    }
}
