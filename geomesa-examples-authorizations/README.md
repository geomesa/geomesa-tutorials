GeoMesa Authorizations Tutorial
============================

This tutorial covers using Accumulo authorizations in your GeoMesa queries. It queries the GDELT dataset, 
which must be ingested before following this tutorial.

This tutorial will show you how to use a default set of authorizations for your queries. It will also show you how to write custom providers that can pull authorizations dynamically. It will demonstrate this by creating an authorization provider for GeoServer.

Complete instructions are available on the [main GeoMesa site](http://geomesa.github.io/2014/06/04/geomesa-authorizations/)

Prerequisites
-------------

Before you begin, you must have the following:

* an instance of Accumulo 1.5.x running on Hadoop 2.2.x
* a local copy of the [Java](http://java.oracle.com/) Development Kit 1.7.x
* Apache [Maven](http://maven.apache.org/) installed
* a GitHub client installed

You must also have ingested the GDELT dataset using GeoMesa, with applied visibility labels.

Download and build GeoMesa
--------------------------

Pick a reasonable directory on your machine, and run:

```
git clone git@github.com:locationtech/geomesa.git
```

From that newly-created directory, run

```
build/mvn clean install
```

NB:  This step is only required, because the GeoMesa artifacts have not yet
been published to a public Maven repository.  With the upcoming 1.0 release of
GeoMesa, these artifacts will be available at LocationTech's Nexus server, and
this download-and-build step will become obsolete.

Download and build this tutorial
--------------------------------

Pick a reasonable directory on your machine, and run:

```
git clone git@github.com:geomesa/geomesa-tutorials.git
```

The ```pom.xml``` file contains an explicit list of dependent libraries that will be bundled together into the final tutorial.  You should confirm
that the versions of Accumulo and Hadoop match what you are running; if it does not match, change the value in the POM.  (NB:  The only reason these libraries
are bundled into the final JAR is that this is easier for most people to do this than it is to set the classpath when running the tutorial.
If you would rather not bundle these dependencies, mark them as ```provided``` in the POM, and update your classpath as appropriate.)

From within the root of the cloned tutorial, run:

```
mvn clean install
```

When this is complete, it should have built a JAR file that contains all of the code you need to run the tutorial.

Run the tutorial
----------------

On the command-line, run:

```
java -cp geomesa-examples-authorizations/target/geomesa-examples-authorizations-${version}.jar com.example.geomesa.authorizations.AuthorizationsTutorial -instanceId <instance> -zookeepers <zoos> -user <user> -password <pwd> -tableName <table> -featureName <feature>
```

where you provide the following arguments:

* ```<instance>```:  the name of your Accumulo instance
* ```<zoos>```:  comma-separated list of your Zookeeper nodes, e.g. zoo1:2181,zoo2:2181,zoo3:2181
* ```<user>```:  the name of an Accumulo user that will execute the scans, e.g. root
* ```<pwd>```:  the password for the previously-mentioned Accumulo user
* ```<table>```:  the name of the Accumulo table that has the GeoMesa GDELT dataset, e.g. gdelt
* ```<feature>```:  the feature name used to ingest the GeoMesa GDELT dataset, e.g. gdelt

You should see several queries run and the results printed out to your console.
