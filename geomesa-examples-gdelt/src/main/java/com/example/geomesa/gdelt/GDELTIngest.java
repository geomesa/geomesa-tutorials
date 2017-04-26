/***********************************************************************
 * Copyright (c) 2014-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package com.example.geomesa.gdelt;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.locationtech.geomesa.jobs.interop.mapreduce.GeoMesaOutputFormat;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GDELTIngest {

    static String INSTANCE_ID = "instanceId";
    static String ZOOKEEPERS = "zookeepers";
    static String USER = "user";
    static String PASSWORD = "password";
    static String AUTHS = "auths";
    static String VISIBILITY = "visibilities";
    static String TABLE_NAME = "tableName";
    static String FEATURE_NAME = "featureName";
    static String INGEST_FILE = "ingestFile";
    static String[] ACCUMULO_CONNECTION_PARAMS = new String[]{INSTANCE_ID,
            ZOOKEEPERS, USER, PASSWORD, AUTHS, VISIBILITY, TABLE_NAME};

    static Options getCommonRequiredOptions() {
        Options options = new Options();
        Option instanceIdOpt = OptionBuilder.withArgName(INSTANCE_ID)
                                         .hasArg()
                                         .isRequired()
                                         .withDescription("accumulo connection parameter instanceId")
                                         .create(INSTANCE_ID);
        Option zookeepersOpt = OptionBuilder.withArgName(ZOOKEEPERS)
                                         .hasArg()
                                         .isRequired()
                                         .withDescription("accumulo connection parameter zookeepers")
                                         .create(ZOOKEEPERS);
        Option userOpt = OptionBuilder.withArgName(USER)
                                         .hasArg()
                                         .isRequired()
                                         .withDescription("accumulo connection parameter user")
                                         .create(USER);
        Option passwordOpt = OptionBuilder.withArgName(PASSWORD)
                                         .hasArg()
                                         .isRequired()
                                         .withDescription("accumulo connection parameter password")
                                         .create(PASSWORD);
        Option authsOpt = OptionBuilder.withArgName(AUTHS)
                                         .hasArg()
                                         .withDescription("accumulo connection parameter auths")
                                         .create(AUTHS);
        Option visibilityOpt = OptionBuilder.withArgName(VISIBILITY)
                                       .hasArg()
                                       .withDescription("visibility label that will be applied to the data")
                                       .create(VISIBILITY);
        Option tableNameOpt = OptionBuilder.withArgName(TABLE_NAME)
                                         .hasArg()
                                         .isRequired()
                                         .withDescription("accumulo connection parameter tableName")
                                         .create(TABLE_NAME);
        Option featureNameOpt = OptionBuilder.withArgName(FEATURE_NAME)
                                         .hasArg()
                                         .isRequired()
                                         .withDescription("name of feature in accumulo table")
                                         .create(FEATURE_NAME);
        options.addOption(instanceIdOpt);
        options.addOption(zookeepersOpt);
        options.addOption(userOpt);
        options.addOption(passwordOpt);
        options.addOption(authsOpt);
        options.addOption(visibilityOpt);
        options.addOption(tableNameOpt);
        options.addOption(featureNameOpt);
        return options;
    }

    public static Map<String, String> getAccumuloDataStoreConf(CommandLine cmd) {
        Map<String , String> dsConf = new HashMap<String , String>();
        for (String param : ACCUMULO_CONNECTION_PARAMS) {
            dsConf.put(param, cmd.getOptionValue(param));
        }
        if (dsConf.get(AUTHS) == null) dsConf.put(AUTHS, "");
        if (dsConf.get(VISIBILITY) == null) dsConf.put(VISIBILITY, "");
        return dsConf;
    }

    public static void main(String [ ] args) throws Exception {
        CommandLineParser parser = new BasicParser();
        Options options = getCommonRequiredOptions();
        Option ingestFileOpt = OptionBuilder.withArgName(INGEST_FILE)
                                         .hasArg()
                                         .isRequired()
                                         .withDescription("ingest tsv file on hdfs")
                                         .create(INGEST_FILE);
        options.addOption(ingestFileOpt);

        CommandLine cmd = parser.parse( options, args);
        Map<String, String> dsConf = getAccumuloDataStoreConf(cmd);

        String featureName = cmd.getOptionValue(FEATURE_NAME);
        SimpleFeatureType featureType = buildGDELTFeatureType(featureName);

        DataStore ds = DataStoreFinder.getDataStore(dsConf);
        ds.createSchema(featureType);

        runMapReduceJob(featureName,
            dsConf,
            new Path(cmd.getOptionValue(INGEST_FILE)));
    }

    private static void runMapReduceJob(String featureName,
                                        Map<String, String> dsConf,
                                        Path mapredCSVFilePath) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJobName("GeoMesa GDELT Ingest");
        job.setJarByClass(GDELTIngest.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(GDELTIngestMapper.class);
        job.setOutputFormatClass(GeoMesaOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SimpleFeature.class);
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, mapredCSVFilePath);
        GeoMesaOutputFormat.configureDataStore(job, dsConf);
        job.getConfiguration().set(FEATURE_NAME, featureName);

        job.submit();

        if (!job.waitForCompletion(true)) {
            throw new Exception("Job failed");
        }
    }

    public static SimpleFeatureType buildGDELTFeatureType(String featureName) throws SchemaException {
        String name = featureName;
        String spec = Joiner.on(",").join(attributes);
        SimpleFeatureType featureType = DataUtilities.createType(name, spec);
        //This tells GeoMesa to use this Attribute as the Start Time index
        featureType.getUserData().put(SimpleFeatureTypes.DEFAULT_DATE_KEY, "SQLDATE");
        return featureType;
    }

    static List<String> attributes = Lists.newArrayList(
            "GLOBALEVENTID:Integer",
            "SQLDATE:Date",
            "MonthYear:Integer",
            "Year:Integer",
            "FractionDate:Float",
            "Actor1Code:String",
            "Actor1Name:String",
            "Actor1CountryCode:String",
            "Actor1KnownGroupCode:String",
            "Actor1EthnicCode:String",
            "Actor1Religion1Code:String",
            "Actor1Religion2Code:String",
            "Actor1Type1Code:String",
            "Actor1Type2Code:String",
            "Actor1Type3Code:String",
            "Actor2Code:String",
            "Actor2Name:String",
            "Actor2CountryCode:String",
            "Actor2KnownGroupCode:String",
            "Actor2EthnicCode:String",
            "Actor2Religion1Code:String",
            "Actor2Religion2Code:String",
            "Actor2Type1Code:String",
            "Actor2Type2Code:String",
            "Actor2Type3Code:String",
            "IsRootEvent:Integer",
            "EventCode:String",
            "EventBaseCode:String",
            "EventRootCode:String",
            "QuadClass:Integer",
            "GoldsteinScale:Float",
            "NumMentions:Integer",
            "NumSources:Integer",
            "NumArticles:Integer",
            "AvgTone:Float",
            "Actor1Geo_Type:Integer",
            "Actor1Geo_FullName:String",
            "Actor1Geo_CountryCode:String",
            "Actor1Geo_ADM1Code:String",
            "Actor1Geo_Lat:Float",
            "Actor1Geo_Long:Float",
            "Actor1Geo_FeatureID:Integer",
            "Actor2Geo_Type:Integer",
            "Actor2Geo_FullName:String",
            "Actor2Geo_CountryCode:String",
            "Actor2Geo_ADM1Code:String",
            "Actor2Geo_Lat:Float",
            "Actor2Geo_Long:Float",
            "Actor2Geo_FeatureID:Integer",
            "ActionGeo_Type:Integer",
            "ActionGeo_FullName:String",
            "ActionGeo_CountryCode:String",
            "ActionGeo_ADM1Code:String",
            "ActionGeo_Lat:Float",
            "ActionGeo_Long:Float",
            "ActionGeo_FeatureID:Integer",
            "DATEADDED:Integer",
            "*geom:Point:srid=4326");

}

