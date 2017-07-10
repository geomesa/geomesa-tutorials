GeoMesa Lambda Quick Start
==========================

This tutorial can get you started with the GeoMesa Lambda data store. Note that the Lambda data store
is for advanced use-cases - see [Overview of the Lambda Data Store](http://www.geomesa.org/documentation/user/lambda/overview.html#lambda-overview)
for details on when to use a Lambda store.

In the spirit of keeping things simple, the code in this tutorial only does a few small things:

1. Establishes a new (static) SimpleFeatureType
2. Prepares the Accumulo table and Kafka topic to store this type of data
3. Creates a thousand example SimpleFeatures
4. Repeatedly updates these SimpleFeatures in the Lambda store through Kafka
5. Persists the final SimpleFeatures to Accumulo

The only dynamic element in the tutorial is the Accumulo and Kafka connection;
it needs to be provided on the command-line when running the code.

Prerequisites
-------------

Before you begin, you must have the following:

* an instance of Accumulo 1.7 or 1.8 running on Hadoop 2.2 or better,
* an Accumulo user that has both create-table and write permissions,
* the GeoMesa Accumulo distributed runtime installed for your Accumulo instance (see [Installing the Accumulo Distributed Runtime Library](http://www.geomesa.org/documentation/user/accumulo/install.html#install-accumulo-runtime)),
* an instance of Kafka 0.9.0.1,
* optionally, a GeoServer instance with the Lambda data store installed (see [Installing GeoMesa Lambda in GeoServer](http://www.geomesa.org/documentation/user/lambda/install.html#install-lambda-geoserver)),
* a local copy of [Java JDK 8](http://www.oracle.com/technetwork/java/javase/downloads/index.html),
* Apache [Maven](http://maven.apache.org/) installed, and
* a [git](http://git-scm.com/) client installed.

Download and Build the Tutorial
-------------------------------

Pick a reasonable directory on your machine, and run:

```bash
$ git clone https://github.com/geomesa/geomesa-tutorials.git
$ cd geomesa-tutorials
```

> :warning: Note: You may need to download a particular release of the tutorials project to target a particular GeoMesa release.

To build, run

```bash
$ mvn clean install -pl geomesa-quickstart-lambda
```

> :warning: Note: Ensure that the version of Accumulo, Hadoop, Kafka etc in the root ``pom.xml`` match your environment.

<span/>

> :warning: Note: Depending on the version, you may also need to build GeoMesa locally. Instructions can be found [here](https://github.com/locationtech/geomesa/).

About this Tutorial
-------------------

The QuickStart operates by inserting 1000 features, and then updating them every 200 milliseconds. After
approximately 30 seconds, the updates stop and the features are persisted to Accumulo.

Run the Tutorial
----------------

On the command-line, run:

```bash
$ java -cp geomesa-quickstart-lambda/target/geomesa-quickstart-lambda-${geomesa.version}.jar \
  com.example.geomesa.lambda.LambdaQuickStart \
  --brokers <brokers>                         \
  --instance <instance>                       \
  --zookeepers <zookeepers>                   \
  --user <user>                               \
  --password <password>                       \
  --catalog <table>
```

where you provide the following arguments:

* ``<brokers>`` the host:port for your Kafka brokers
* ``<instance>`` the name of your Accumulo instance
* ``<zookeepers>`` your Zookeeper nodes, separated by commas
* ``<user>`` the name of an Accumulo user that has permissions to create, read and write tables
* ``<password>`` the password for the previously-mentioned Accumulo user
* ``<table>`` the name of the destination table that will accept these test records; this table should either not exist or should be empty

> :warning: Note: If you have set up the GeoMesa Accumulo distributed runtime to be isolated within a namespace (see
> [Namespace Install](http://www.geomesa.org/documentation/user/accumulo/install.html#install-accumulo-runtime-namespace)) the value of ``<table>``
> should include the namespace (e.g. ``myNamespace.geomesa``).

Once you run the quick start, it will prompt you to load the layer in geoserver. Using the same connection
parameters you used for the quick start, register a new data store according to [Using the Lambda Data Store in GeoServer](http://www.geomesa.org/documentation/user/lambda/geoserver.html#create-lambda-ds-geoserver).
After saving the store, you should be able to publish the ``lambda-quick-start`` layer. Open the layer preview for
the layer, then proceed with the quick start run.

As the quick start runs, you should be able to refresh the layer preview page and see the features moving across
the map. After approximately 30 seconds, the updates will stop, and the features will be persisted to Accumulo.

Transient vs Persistent Features
--------------------------------

The layer preview will merge the results of features from Kafka with features from Accumulo. You may disable
results from one of the source by using the ``viewparams`` parameter:

```bash
...&viewparams=LAMBDA_QUERY_TRANSIENT:false
...&viewparams=LAMBDA_QUERY_PERSISTENT:false
```

While the quick start is running, all the features should be returned from the transient store (Kafka). After the quick
start finishes, all the feature should be returned from the persistent store (Accumulo). You can play with the
``viewparams`` to see the difference.

Looking at the Code
-------------------

Looking at the source code, you can see that normal GeoTools ``FeatureWriters`` are used; feature persistence
is managed transparently for you.

Re-Running the Quick Start
--------------------------

The quick start relies on not having any existing state when it runs. This can cause issues with Kafka, which
by default does not delete topics when requested. To re-run the quick start, first ensure that your Kafka
instance will delete topics by setting the configuration ``delete.topic.enable=true`` in your server properties.
Then use the Lamdba command-line tools (see [Setting up the Lambda Command Line Tools](http://www.geomesa.org/documentation/user/lambda/install.html#setting-up-lambda-commandline)) to remove the quick start schema:

```bash
$ geomesa-lambda remove-schema -f lambda-quick-start ...
```
