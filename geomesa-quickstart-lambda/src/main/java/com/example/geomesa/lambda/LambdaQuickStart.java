/***********************************************************************
 * Copyright (c) 2016-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package com.example.geomesa.lambda;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureWriter;
import org.geotools.factory.Hints;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore;
import org.locationtech.geomesa.lambda.data.LambdaDataStore;
import org.locationtech.geomesa.lambda.data.LambdaDataStoreFactory;
import org.locationtech.geomesa.lambda.data.LambdaDataStoreFactory$Params$Accumulo$;
import org.locationtech.geomesa.lambda.data.LambdaDataStoreFactory$Params$Kafka$;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class LambdaQuickStart implements Runnable {

  // reads and parse the command line args
  @SuppressWarnings("AccessStaticViaInstance")
  public static Map<String, String> parseOptions(String[] args) {
    Options options = new Options();

    options.addOption(OptionBuilder.hasArg()
                                   .withArgName("brokers")
                                   .withDescription("The comma-separated list of Kafka brokers, e.g. localhost:9092")
                                   .isRequired()
                                   .withLongOpt("brokers")
                                   .create("b"));

    options.addOption(OptionBuilder.hasArg()
                                   .withArgName("zookeepers")
                                   .withDescription("The comma-separated list of Zookeeper nodes that support your Kafka and Accumulo instances, e.g.: zoo1:2181,zoo2:2181,zoo3:2181")
                                   .isRequired()
                                   .withLongOpt("zookeepers")
                                   .create("z"));

    options.addOption(OptionBuilder.hasArg()
                                   .withArgName("instance")
                                   .withDescription("The name of your Accumulo instance")
                                   .isRequired()
                                   .withLongOpt("instance")
                                   .create("i"));

    options.addOption(OptionBuilder.hasArg()
                                   .withArgName("user")
                                   .withDescription("The user used to connect to your Accumulo instance")
                                   .isRequired()
                                   .withLongOpt("user")
                                   .create("u"));

    options.addOption(OptionBuilder.hasArg()
                                   .withArgName("password")
                                   .withDescription("The password for your Accumulo instance")
                                   .isRequired()
                                   .withLongOpt("password")
                                   .create("p"));

    options.addOption(OptionBuilder.hasArg()
                                   .withArgName("catalog")
                                   .withDescription("The catalog table to store Accumulo data, e.g. geomesa.my_catalog")
                                   .isRequired()
                                   .withLongOpt("catalog")
                                   .create("c"));

    Map<String, String> map = new HashMap<>();

    CommandLine cmd = null;
    try {
      cmd = new BasicParser().parse(options, args);
    } catch (MissingOptionException e) {
      System.err.println(e.getMessage());
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp( "LambdaQuickStart", options );
      return null;
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }

    String zookeepers = cmd.getOptionValue("z");

    map.put(LambdaDataStoreFactory$Params$Accumulo$.MODULE$.ZookeepersParam().getName(), zookeepers);
    map.put(LambdaDataStoreFactory$Params$Accumulo$.MODULE$.InstanceParam().getName(), cmd.getOptionValue("i"));
    map.put(LambdaDataStoreFactory$Params$Accumulo$.MODULE$.UserParam().getName(), cmd.getOptionValue("u"));
    map.put(LambdaDataStoreFactory$Params$Accumulo$.MODULE$.PasswordParam().getName(), cmd.getOptionValue("p"));
    map.put(LambdaDataStoreFactory$Params$Accumulo$.MODULE$.CatalogParam().getName(), cmd.getOptionValue("c"));

    map.put(LambdaDataStoreFactory$Params$Kafka$.MODULE$.BrokersParam().getName(), cmd.getOptionValue("b"));
    map.put(LambdaDataStoreFactory$Params$Kafka$.MODULE$.ZookeepersParam().getName(), zookeepers);

    map.put(LambdaDataStoreFactory.Params$.MODULE$.ExpiryParam().getName(), "1s");

    return map;
  }

  public static void main(String[] args) throws Exception {
    Map<String, String> params = parseOptions(args);
    if (params == null) {
      System.exit(1);
    } else {
      try {
        new LambdaQuickStart(params, System.out, System.in).run();
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(2);
      }
      System.exit(0);
    }
  }

  private LambdaDataStore ds;
  private PrintStream out;
  private InputStream in;

  public LambdaQuickStart(Map<String, String> params, PrintStream out, InputStream in) {
    System.setProperty("geomesa.lambda.persist.interval", "2s");
    try {
      ds = (LambdaDataStore) DataStoreFinder.getDataStore(params);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.out = out;
    this.in = in;
  }

  @Override
  public void run() {
    try {
      // create the schema
      final String sftName = "lambda-quick-start";
      final String sftSchema = "name:String,age:Int,dtg:Date,*geom:Point:srid=4326";
      SimpleFeatureType sft = SimpleFeatureTypes.createType(sftName, sftSchema);

      if (ds.getSchema(sftName) != null) {
        out.println("'" + sftName + "' feature type already exists - quick start will not work correctly");
        out.println("Delete it and re-run");
        return;
      }

      out.println("Creating feature type '" + sftName + "'");

      ds.createSchema(sft);

      out.println("Feature type created - register the layer '" + sftName + "' in geoserver then hit <enter> to continue");
      in.read();

      SimpleFeatureWriter writer = ds.getFeatureWriterAppend(sftName, Transaction.AUTO_COMMIT);

      out.println("Writing features to Kafka... refresh GeoServer layer preview to see changes");

      // creates and adds SimpleFeatures to the producer every 1/5th of a second

      final int COUNT = 1000;
      final int MIN_X = -180;
      final int MAX_X = 180;
      final int MIN_Y = -90;
      final int MAX_Y = 90;
      final int DX = 2;
      final String[] PEOPLE_NAMES = {"James", "John", "Peter", "Hannah", "Claire", "Gabriel"};
      final long SECONDS_PER_YEAR = 365L * 24L * 60L * 60L;
      final DateTime MIN_DATE = new DateTime(2015, 1, 1, 0, 0, 0, DateTimeZone.forID("UTC"));
      final Random random = new Random();

      int numUpdates = (MAX_X - MIN_X) / DX;
      for (int j = 0; j < numUpdates; j++) {
        for (int i = 0; i < COUNT; i++) {
          SimpleFeature feature = writer.next();
          feature.setAttribute(0, PEOPLE_NAMES[i % PEOPLE_NAMES.length]); // name
          feature.setAttribute(1, (int) Math.round(random.nextDouble() * 110)); // age
          feature.setAttribute(2, MIN_DATE.plusSeconds((int) Math.round(random.nextDouble() * SECONDS_PER_YEAR)).toDate()); // dtg
          feature.setAttribute(3, "POINT(" + (MIN_X + (DX * j)) + " " + (MIN_Y + ((MAX_Y - MIN_Y) / ((double) COUNT)) * i) + ")"); // geom
          feature.getUserData().put(Hints.PROVIDED_FID, String.format("%04d", i));
          writer.write();
        }
        Thread.sleep(200);
      }

      writer.close();

      out.println("Waiting for expiry and persistence...");

      long total = 0, persisted = 0;
      do {
        long newTotal = (long) ds.stats().getCount(sft, Filter.INCLUDE, true).get();
        long newPersisted = (long)((AccumuloDataStore) ds.persistence()).stats().getCount(sft, Filter.INCLUDE, true).get();
        if (newTotal != total || newPersisted != persisted) {
          total = newTotal;
          persisted = newPersisted;
          out.println("Total features: " + total + ", features persisted to Accumulo: " + persisted);
        }
        Thread.sleep(100);
      } while (persisted < COUNT || total > COUNT);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      ds.dispose();
    }
  }
}
