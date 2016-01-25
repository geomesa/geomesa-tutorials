GeoMesa Accumulo Quick-Start
============================

This tutorial is the fastest and easiest way to get started with GeoMesa.  It is
a good stepping-stone on the path to the other tutorials that present
increasingly involved examples of how to use GeoMesa.

In the spirit of keeping things simple, the code in this tutorial only does a
few small things:

1.  establishes a new (static) SimpleFeatureType
2.  prepares the Accumulo table to store this type of data
3.  creates a few hundred example SimpleFeatures
4.  writes these SimpleFeatures to the Accumulo table
5.  queries for a given geographic rectangle, time range, and attribute filter,
    writing out the entries in the result set

The only dynamic element in the tutorial is the Accumulo destination; that is
a property that you provide on the command-line when running the code.

Prerequisites
-------------

Before you begin, you must have the following:

* an instance of Accumulo 1.5 or 1.6 running on Hadoop 2.2.x
* an Accumulo user that has both create-table and write permissions
* a local copy of the [Java](http://java.oracle.com/) Development Kit 1.7.x
* Apache [Maven](http://maven.apache.org/) installed
* a GitHub client installed

Download and Build the Tutorial
--------------------------

Pick a reasonable directory on your machine, and run:

```bash
git@github.com:geomesa/geomesa-tutorials.git
cd geomesa-tutorials
```

To build, run

```
mvn clean install -pl geomesa-quickstart-accumulo
```

> :warning: Note: Ensure that the version of Accumulo, Hadoop, etc in the root `pom.xml` match your environment.

<span/>

> :warning: Note: depending on the version, you may also need to build GeoMesa locally.
> Instructions can be found [here](https://github.com/locationtech/geomesa/).

About this Tutorial
-------------------

The QuickStart operates by inserting and then querying 1000 features.  After the insertions are complete,
a sequence of queries are run to demonstrate different types of queries possible via the GeoTools API.

Run the Tutorial
----------------

On the command-line, run:

```
java -cp geomesa-quickstart-accumulo/target/geomesa-quickstart-accumulo-${geomesa.version}.jar com.example.geomesa.accumulo.AccumuloQuickStart -instanceId <instance> -zookeepers <zookeepers> -user <user> -password <password> -tableName <table>
```

where you provide the following arguments:

* ```<instance>``` the name of your Accumulo instance
* ```<zookeepers>``` your Zookeeper nodes, separated by commas
* ```<user>``` the name of an Accumulo user that has permissions to create, read and write tables
* ```<password>``` the password for the previously-mentioned Accumulo user
* ```<table>``` the name of the destination table that will accept these test records; this table should either not exist or should be empty

You should see output similar to the following (not including some of Maven's output and log4j's warnings):

    Creating feature-type (schema):  QuickStart
    Creating new features
    Inserting new features
    Submitting query
    1.  Bierce|640|Sun Sep 14 15:48:25 EDT 2014|POINT (-77.36222958792739 -37.13013846773835)|null
    2.  Bierce|886|Tue Jul 22 14:12:36 EDT 2014|POINT (-76.59795732474399 -37.18420917493149)|null
    3.  Bierce|925|Sun Aug 17 23:28:33 EDT 2014|POINT (-76.5621106573523 -37.34321201566148)|null
    4.  Bierce|589|Sat Jul 05 02:02:15 EDT 2014|POINT (-76.88146600670152 -37.40156607152168)|null
    5.  Bierce|394|Fri Aug 01 19:55:05 EDT 2014|POINT (-77.42555615743139 -37.26710898726304)|null
    6.  Bierce|931|Fri Jul 04 18:25:38 EDT 2014|POINT (-76.51304097832912 -37.49406125975311)|null
    7.  Bierce|322|Tue Jul 15 17:09:42 EDT 2014|POINT (-77.01760098223343 -37.30933767159561)|null
    8.  Bierce|343|Wed Aug 06 04:59:22 EDT 2014|POINT (-76.66826220670282 -37.44503877750368)|null
    9.  Bierce|259|Thu Aug 28 15:59:30 EDT 2014|POINT (-76.90122194030118 -37.148525741002466)|null
    Submitting secondary index query
    Feature ID Observation.859 | Who: Bierce
    Feature ID Observation.355 | Who: Bierce
    Feature ID Observation.940 | Who: Bierce
    Feature ID Observation.631 | Who: Bierce
    Feature ID Observation.817 | Who: Bierce
    Submitting secondary index query with sorting (sorted by 'What' descending)
    Feature ID Observation.999 | Who: Addams | What: 999
    Feature ID Observation.996 | Who: Addams | What: 996
    Feature ID Observation.993 | Who: Addams | What: 993
    Feature ID Observation.990 | Who: Addams | What: 990
    Feature ID Observation.987 | Who: Addams | What: 987
