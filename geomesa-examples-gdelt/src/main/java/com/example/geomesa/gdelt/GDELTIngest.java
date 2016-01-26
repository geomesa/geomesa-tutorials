package com.example.geomesa.gdelt;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.locationtech.geomesa.accumulo.index.Constants;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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

    public static String getVersion() {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("project.properties");
        Properties p = new Properties();
        try {
            p.load(is);
        }
        catch (IOException e) {
            System.err.println("Exception when loading properties: " + e);
        }

        return p.getProperty("project.version");
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
        job.setMapperClass(GDELTIngestMapper.class);
        job.setOutputFormatClass(AccumuloFileOutputFormat.class);
        job.setJobName("GeoMesa GDELT Ingest");
        Configuration conf = job.getConfiguration();
        for (String key : dsConf.keySet()) {
            conf.set(key, dsConf.get(key));
        }
        conf.set(FEATURE_NAME, featureName);
        FileSystem fs = FileSystem.get(conf);
        FileInputFormat.setInputPaths(job, mapredCSVFilePath);
        String jarFile = "geomesa-examples-gdelt-" + getVersion() + ".jar";

        Path tmpPath = new Path("///tmp");
        if (!fs.exists(tmpPath)) {
            fs.mkdirs(tmpPath);
        }
        Path outputDir = new Path("///tmp", "geomesa-gdelt-output");
        if (fs.exists(outputDir)) {
          // remove this directory, if it already exists
            fs.delete(outputDir, true);
        }
        Path hdfsJarPath = new Path("///tmp", jarFile);
        if (fs.exists(hdfsJarPath)) {
          // remove this jar, if it already exists
            fs.delete(hdfsJarPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputDir);
        fs.copyFromLocalFile(new Path("target/" + jarFile), hdfsJarPath);
        for (FileStatus path : fs.listStatus(hdfsJarPath)) {
            job.addArchiveToClassPath(new Path(path.getPath().toUri().getPath()));
        }

        job.submit();

        if (!job.waitForCompletion(true)) {
            throw new Exception("Job failed");
        }
    }

    private static SimpleFeatureType buildGDELTFeatureType(String featureName) throws SchemaException {
        String name = featureName;
        String spec = Joiner.on(",").join(attributes);
        SimpleFeatureType featureType = DataUtilities.createType(name, spec);
        //This tells GeoMesa to use this Attribute as the Start Time index
        featureType.getUserData().put(Constants.SF_PROPERTY_START_TIME, "SQLDATE");
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

