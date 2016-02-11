# GeoMesa Transformations

GeoMesa allows users to perform
[relational projections](http://en.wikipedia.org/wiki/Projection_%28relational_algebra%29)
on query results. We call these "transformations" to distinguish them from the overloaded
term "projection" which has a different meaning in a spatial context. These
transformations have the following uses and advantages:

1. Subset to specified columns - reduces network overhead of returning results
2. Rename specified columns - alters the schema of data on the fly
3. Compute new attributes from one or more original attributes - adds derived fields to results

The transformations are applied in parallel across the cluster thus making them
very fast. They are analogous to the map tasks in a map-reduce job. Transformations are
also extensible; developers can implement new functions and plug them into the system
using standard mechanisms from [Geotools](http://www.geotools.org/).  

**Note:** when this tutorial refers to "projections", it means in the relational
sense - see [Projection - Relational Algebra](http://en.wikipedia.org/wiki/Projection_\(relational_algebra\)).
Projection also has [many other meanings](http://en.wikipedia.org/wiki/Projection_\(disambiguation\))
in spatial discussions - they are not used in this tutorial. Although projections can also
modify an attribute's value, in this tutorial we will refer to such modifications as
"transformations" to keep things clearer.

This tutorial will show you how to write custom Java code using GeoMesa to do the following:

1. Query previously-ingested data.
2. Apply [relational projections](http://en.wikipedia.org/wiki/Projection_%28relational_algebra%29) to your query results.
3. Apply transformations to your query results.

## Prerequisites

You will need:

* an instance of Accumulo 1.5 or 1.6 running on Hadoop 2.2.x
* an Accumulo user that has appropriate permissions to query your data
* [Java JDK 8](http://www.oracle.com/technetwork/java/javase/downloads/index.html)
* [Apache Maven](http://maven.apache.org/) 3.2.2 or better
* a [git](http://git-scm.com/) client

This tutorial queries the GDELT data set. Instructions on ingesting GDELT data are available [here](../geomesa-examples-gdelt).

> :warning: Before continuing, ingest the GDELT data set described in the GeoMesa GDELT [tutorial](../geomesa-examples-gdelt).

## Download and Build the Tutorial

Pick a reasonable directory on your machine, and run:

```bash
$ git clone git@github.com:geomesa/geomesa-tutorials.git
$ cd geomesa-tutorials
```

To build, run

```bash
$ mvn clean install -pl geomesa-examples-transformations
```

> :warning: Note: ensure that the version of Accumulo, Hadoop, etc in the root `pom.xml` match your environment.

<span/>

> :warning: Note: depending on the version, you may also need to build GeoMesa locally.
> Instructions can be found [here](https://github.com/locationtech/geomesa/).

## Run the Tutorial

> :warning: Before continuing, ensure that you have ingested the GDELT data set described
in the GeoMesa GDELT [tutorial](../geomesa-examples-gdelt).

On the command line, run:

```bash
$ java -cp geomesa-examples-transformations/target/geomesa-examples-transformations-<version>.jar \
    com.example.geomesa.transformations.QueryTutorial \
    -instanceId <instance>                            \
    -zookeepers <zoos>                                \
    -user <user>                                      \
    -password <pwd>                                   \
    -tableName <table>                                \
    -featureName <feature>
```

where you provide the following arguments:

* `<instance>` the name of your Accumulo instance
* `<zoos>` comma-separated list of your Zookeeper nodes, e.g. `zoo1:2181,zoo2:2181,zoo3:2181`
* `<user>` the name of an Accumulo user that will execute the scans, e.g. `root`
* `<pwd>` the password for the previously-mentioned Accumulo user
* `<table>` the name of the Accumulo table that has the GeoMesa GDELT dataset, e.g. `gdelt` if you followed the GDELT tutorial
* `<feature>` the feature name used to ingest the GeoMesa GDELT dataset, e.g. `event` if you followed the GDELT tutorial 

You should see several queries run and the results printed out to your console.

## Insight into How the Tutorial Works

The code for querying and projections is available in the class
`com.example.geomesa.transformations.QueryTutorial`. The source code is meant to be accessible,
but the following is a high-level breakdown of the relevant methods:

* `basicQuery` executes a base filter without any further options. All attributes
are returned in the data set.
* `basicProjectionQuery` executes a base filter but specifies a subset of attributes to return.
* `basicTransformationQuery` executes a base filter and transforms one of the attributes that
is returned.
* `renamedTransformationQuery` executes a base filter and transforms one of the attributes,
returning it in a separate derived attribute.
* `mutliFieldTransformationQuery` executes a base filter and transforms two attributes into
a single derived attributes.
* `geometricTransformationQuery` executes a base filter and transforms the geometry returned
from a point into a polygon by buffering it. 

Additional transformation functions are listed [here](http://docs.geotools.org/latest/userguide/library/main/filter.html).

*Please note that currently not all functions are supported by GeoMesa.*

## Sample Code and Output

The following code snippets show the basic aspects of creating queries for GeoMesa.

#### Create a basic query with no projections

This query does not use any projections or transformations. Note that all attributes
are returned in the results.

```java
Query query = new Query(simpleFeatureTypeName, cqlFilter);
```

**Output**

| Result | GLOBALEVENTID | SQLDATE | MonthYear | Year | FractionDate | Actor1Code | Actor1Name | Actor1CountryCode | Actor1KnownGroupCode | Actor1EthnicCode | Actor1Religion1Code | Actor1Religion2Code | Actor1Type1Code | Actor1Type2Code | Actor1Type3Code | Actor2Code | Actor2Name | Actor2CountryCode | Actor2KnownGroupCode | Actor2EthnicCode | Actor2Religion1Code | Actor2Religion2Code | Actor2Type1Code | Actor2Type2Code | Actor2Type3Code | IsRootEvent | EventCode | EventBaseCode | EventRootCode | QuadClass | GoldsteinScale | NumMentions | NumSources | NumArticles | AvgTone | Actor1Geo_Type | Actor1Geo_FullName | Actor1Geo_CountryCode | Actor1Geo_ADM1Code | Actor1Geo_Lat | Actor1Geo_Long | Actor1Geo_FeatureID | Actor2Geo_Type | Actor2Geo_FullName | Actor2Geo_CountryCode | Actor2Geo_ADM1Code | Actor2Geo_Lat | Actor2Geo_Long | Actor2Geo_FeatureID | ActionGeo_Type | ActionGeo_FullName | ActionGeo_CountryCode | ActionGeo_ADM1Code | ActionGeo_Lat | ActionGeo_Long | ActionGeo_FeatureID | DATEADDED | geom |
| ------ | ------------- | ------- | --------- | ---- | ------------ | ---------- | ---------- | ----------------- | -------------------- | ---------------- | ------------------- | ------------------- | --------------- | --------------- | --------------- | ---------- | ---------- | ----------------- | -------------------- | ---------------- | ------------------- | ------------------- | --------------- | --------------- | --------------- | ----------- | --------- | ------------- | ------------- | --------- | -------------- | ----------- | ---------- | ----------- | ------- | -------------- | ------------------ | --------------------- | ------------------ | ------------- | -------------- | ------------------- | -------------- | ------------------ | --------------------- | ------------------ | ------------- | -------------- | ------------------- | -------------- | ------------------ | --------------------- | ------------------ | ------------- | -------------- | ------------------- | --------- | ---- |
| 1 | 284464526 | Sun&nbsp;Feb&nbsp;02&nbsp;00:00:00&nbsp;EST&nbsp;2014 | 201402 | 2014 | 2014.0876 | USA | UNITED&nbsp;STATES | USA |  |  |  |  |  |  |  | USAGOV | UNITED&nbsp;STATES | USA |  |  |  |  | GOV |  |  | 0 | 010 | 010 | 01 | 1 | 0.0 | 2 | 1 | 2 | 2.6362038 | 4 | Kyiv,&nbsp;Kyyiv,&nbsp;Misto,&nbsp;Ukraine | UP | UP12 | 50.4333 | 30.5167 | -1044367 | 1 | United&nbsp;States | US | US | 38.0 | -97.0 | null | 1 | United&nbsp;States | US | US | 38.0 | -97.0 | null | 20140202 | POINT&nbsp;(30.5167&nbsp;50.4333) |
| 2 | 284466704 | Sun&nbsp;Feb&nbsp;02&nbsp;00:00:00&nbsp;EST&nbsp;2014 | 201402 | 2014 | 2014.0876 | USAGOV | UNITED&nbsp;STATES | USA |  |  |  |  | GOV |  |  | USA | UNITED&nbsp;STATES | USA |  |  |  |  |  |  |  | 1 | 036 | 036 | 03 | 1 | 4.0 | 4 | 1 | 4 | 1.5810276 | 1 | Ukraine | UP | UP | 49.0 | 32.0 | null | 1 | Ukraine | UP | UP | 49.0 | 32.0 | null | 1 | Ukraine | UP | UP | 49.0 | 32.0 | null | 20140202 | POINT&nbsp;(32&nbsp;49) |
| 3 | 284427971 | Sun&nbsp;Feb&nbsp;02&nbsp;00:00:00&nbsp;EST&nbsp;2014 | 201402 | 2014 | 2014.0876 | IGOUNO | UNITED&nbsp;NATIONS |  | UNO |  |  |  | IGO |  |  | USA | UNITED&nbsp;STATES | USA |  |  |  |  |  |  |  | 0 | 012 | 012 | 01 | 1 | -0.4 | 27 | 3 | 27 | 1.0064903 | 4 | Kiev,&nbsp;Ukraine&nbsp;(general),&nbsp;Ukraine | UP | UP00 | 50.4333 | 30.5167 | -1044367 | 4 | Kiev,&nbsp;Ukraine&nbsp;(general),&nbsp;Ukraine | UP | UP00 | 50.4333 | 30.5167 | -1044367 | 4 | Kiev,&nbsp;Ukraine&nbsp;(general),&nbsp;Ukraine | UP | UP00 | 50.4333 | 30.5167 | -1044367 | 20140202 | POINT&nbsp;(30.5167&nbsp;50.4333) |
| 4 | 284466607 | Sun&nbsp;Feb&nbsp;02&nbsp;00:00:00&nbsp;EST&nbsp;2014 | 201402 | 2014 | 2014.0876 | USAGOV | UNITED&nbsp;STATES | USA |  |  |  |  | GOV |  |  | UKR | UKRAINE | UKR |  |  |  |  |  |  |  | 1 | 100 | 100 | 10 | 3 | -5.0 | 2 | 1 | 2 | 7.826087 | 1 | Ukraine | UP | UP | 49.0 | 32.0 | null | 1 | Ukraine | UP | UP | 49.0 | 32.0 | null | 1 | Ukraine | UP | UP | 49.0 | 32.0 | null | 20140202 | POINT&nbsp;(32&nbsp;49) |
| 5 | 284464187 | Sun&nbsp;Feb&nbsp;02&nbsp;00:00:00&nbsp;EST&nbsp;2014 | 201402 | 2014 | 2014.0876 | USA | UNITED&nbsp;STATES | USA |  |  |  |  |  |  |  | UKR | UKRAINE | UKR |  |  |  |  |  |  |  | 0 | 111 | 111 | 11 | 3 | -2.0 | 5 | 1 | 5 | 1.4492754 | 4 | Kiev,&nbsp;Ukraine&nbsp;(general),&nbsp;Ukraine | UP | UP00 | 50.4333 | 30.5167 | -1044367 | 4 | Kiev,&nbsp;Ukraine&nbsp;(general),&nbsp;Ukraine | UP | UP00 | 50.4333 | 30.5167 | -1044367 | 4 | Kiev,&nbsp;Ukraine&nbsp;(general),&nbsp;Ukraine | UP | UP00 | 50.4333 | 30.5167 | -1044367 | 20140202 | POINT&nbsp;(30.5167&nbsp;50.4333) |

#### Create a query with a projection for two attributes

This query uses a projection to only return the 'Actor1Name' and 'geom' attributes.

```java
String[] properties = new String[] {"Actor1Name", "geom"};
Query query = new Query(simpleFeatureTypeName, cqlFilter, properties);
```

**Output**

| Result | Actor1Name | geom |
| ------ | ---------- | ---- |
| 1 | UNITED STATES | POINT (32 49) |
| 2 | UNITED STATES | POINT (30.5167 50.4333) |
| 3 | UNITED STATES | POINT (30.5167 50.4333) |
| 4 | UNITED STATES | POINT (30.5167 50.4333) |
| 5 | UNITED STATES | POINT (30.5167 50.4333) |

#### Create a query with an attribute transformation

This query performs a transformation on the 'Actor1Name' attribute, to print it in a more user-friendly format.

```java
String[] properties = new String[] {"Actor1Name=strCapitalize(Actor1Name)", "geom"};
Query query = new Query(simpleFeatureTypeName, cqlFilter, properties);
```

**Output**

| Result | geom | Actor1Name |
| ------ | ---- | ---------- |
| 1 | POINT (30.5167 50.4333) | United States |
| 2 | POINT (32 49) | United States |
| 3 | POINT (32 49) | United States |
| 4 | POINT (30.5167 50.4333) | United States |
| 5 | POINT (30.5167 50.4333) | United States |

#### Create a query with a derived attribute

This query creates a new attribute called 'derived' based off a join of the 'Actor1Name' and
'Actor1Geo_FullName' attribute. This could be used to show the actor and location of the event, for example.

```java
String property = "derived=strConcat(Actor1Name,strConcat(' - ',Actor1Geo_FullName)),geom";
String[] properties = new String[] { property };
Query query = new Query(simpleFeatureTypeName, cqlFilter, properties);
```

**Output**

| Result | geom | derived |
| ------ | ---- | ------- |
| 1 | POINT (30.5167 50.4333) | UNITED STATES - Kyiv, Kyyiv, Misto, Ukraine |
| 2 | POINT (32 49) | UNITED STATES - Ukraine |
| 3 | POINT (30.5167 50.4333) | UNITED STATES - Kiev, Ukraine (general), Ukraine |
| 4 | POINT (32 49) | UNITED STATES - Ukraine |
| 5 | POINT (30.5167 50.4333) | UNITED NATIONS - Kiev, Ukraine (general), Ukraine |

#### Create a query with a geometric transformation

This query performs a geometric transformation on the points returned, buffering them
by a fixed amount. This could be used to estimate an area of impact around a particular event, for example.

```java
String[] properties = new String[] {"geom,derived=buffer(geom, 2)"};
Query query = new Query(simpleFeatureTypeName, cqlFilter, properties);
```

**Output**

| Result | geom | derived |
| ------ | ---- | ------- |
| 1 | POINT&nbsp;(30.5167&nbsp;50.4333) | POLYGON&nbsp;((32.5167&nbsp;50.4333,&nbsp;32.478270560806465&nbsp;50.04311935596775,&nbsp;32.36445906502257&nbsp;49.66793313526982,&nbsp;32.17963922460509&nbsp;49.3221595339608,&nbsp;31.930913562373096&nbsp;49.01908643762691,&nbsp;31.627840466039206&nbsp;48.77036077539491,&nbsp;31.28206686473018&nbsp;48.58554093497743,&nbsp;30.906880644032256&nbsp;48.47172943919354,&nbsp;30.5167&nbsp;48.4333,&nbsp;30.126519355967744&nbsp;48.47172943919354,&nbsp;29.75133313526982&nbsp;48.58554093497743,&nbsp;29.405559533960798&nbsp;48.77036077539491,&nbsp;29.102486437626904&nbsp;49.01908643762691,&nbsp;28.85376077539491&nbsp;49.3221595339608,&nbsp;28.668940934977428&nbsp;49.66793313526983,&nbsp;28.55512943919354&nbsp;50.04311935596775,&nbsp;28.5167&nbsp;50.4333,&nbsp;28.55512943919354&nbsp;50.82348064403226,&nbsp;28.668940934977428&nbsp;51.198666864730185,&nbsp;28.85376077539491&nbsp;51.54444046603921,&nbsp;29.102486437626908&nbsp;51.8475135623731,&nbsp;29.405559533960798&nbsp;52.09623922460509,&nbsp;29.751333135269824&nbsp;52.281059065022575,&nbsp;30.126519355967748&nbsp;52.39487056080647,&nbsp;30.516700000000004&nbsp;52.4333,&nbsp;30.906880644032263&nbsp;52.39487056080646,&nbsp;31.282066864730186&nbsp;52.281059065022575,&nbsp;31.62784046603921&nbsp;52.09623922460509,&nbsp;31.9309135623731&nbsp;51.847513562373095,&nbsp;32.1796392246051&nbsp;51.5444404660392,&nbsp;32.36445906502258&nbsp;51.19866686473018,&nbsp;32.478270560806465&nbsp;50.82348064403225,&nbsp;32.5167&nbsp;50.4333)) |
| 2 | POINT&nbsp;(30.5167&nbsp;50.4333) | POLYGON&nbsp;((32.5167&nbsp;50.4333,&nbsp;32.478270560806465&nbsp;50.04311935596775,&nbsp;32.36445906502257&nbsp;49.66793313526982,&nbsp;32.17963922460509&nbsp;49.3221595339608,&nbsp;31.930913562373096&nbsp;49.01908643762691,&nbsp;31.627840466039206&nbsp;48.77036077539491,&nbsp;31.28206686473018&nbsp;48.58554093497743,&nbsp;30.906880644032256&nbsp;48.47172943919354,&nbsp;30.5167&nbsp;48.4333,&nbsp;30.126519355967744&nbsp;48.47172943919354,&nbsp;29.75133313526982&nbsp;48.58554093497743,&nbsp;29.405559533960798&nbsp;48.77036077539491,&nbsp;29.102486437626904&nbsp;49.01908643762691,&nbsp;28.85376077539491&nbsp;49.3221595339608,&nbsp;28.668940934977428&nbsp;49.66793313526983,&nbsp;28.55512943919354&nbsp;50.04311935596775,&nbsp;28.5167&nbsp;50.4333,&nbsp;28.55512943919354&nbsp;50.82348064403226,&nbsp;28.668940934977428&nbsp;51.198666864730185,&nbsp;28.85376077539491&nbsp;51.54444046603921,&nbsp;29.102486437626908&nbsp;51.8475135623731,&nbsp;29.405559533960798&nbsp;52.09623922460509,&nbsp;29.751333135269824&nbsp;52.281059065022575,&nbsp;30.126519355967748&nbsp;52.39487056080647,&nbsp;30.516700000000004&nbsp;52.4333,&nbsp;30.906880644032263&nbsp;52.39487056080646,&nbsp;31.282066864730186&nbsp;52.281059065022575,&nbsp;31.62784046603921&nbsp;52.09623922460509,&nbsp;31.9309135623731&nbsp;51.847513562373095,&nbsp;32.1796392246051&nbsp;51.5444404660392,&nbsp;32.36445906502258&nbsp;51.19866686473018,&nbsp;32.478270560806465&nbsp;50.82348064403225,&nbsp;32.5167&nbsp;50.4333)) |
| 3 | POINT&nbsp;(32&nbsp;49) | POLYGON&nbsp;((34&nbsp;49,&nbsp;33.961570560806464&nbsp;48.609819355967744,&nbsp;33.84775906502257&nbsp;48.23463313526982,&nbsp;33.66293922460509&nbsp;47.8888595339608,&nbsp;33.41421356237309&nbsp;47.58578643762691,&nbsp;33.1111404660392&nbsp;47.33706077539491,&nbsp;32.76536686473018&nbsp;47.15224093497743,&nbsp;32.390180644032256&nbsp;47.038429439193536,&nbsp;32&nbsp;47,&nbsp;31.609819355967744&nbsp;47.038429439193536,&nbsp;31.23463313526982&nbsp;47.15224093497743,&nbsp;30.888859533960797&nbsp;47.33706077539491,&nbsp;30.585786437626904&nbsp;47.58578643762691,&nbsp;30.33706077539491&nbsp;47.8888595339608,&nbsp;30.152240934977428&nbsp;48.234633135269824,&nbsp;30.03842943919354&nbsp;48.609819355967744,&nbsp;30&nbsp;49,&nbsp;30.03842943919354&nbsp;49.390180644032256,&nbsp;30.152240934977428&nbsp;49.76536686473018,&nbsp;30.33706077539491&nbsp;50.11114046603921,&nbsp;30.585786437626908&nbsp;50.4142135623731,&nbsp;30.888859533960797&nbsp;50.66293922460509,&nbsp;31.234633135269824&nbsp;50.84775906502257,&nbsp;31.609819355967748&nbsp;50.961570560806464,&nbsp;32.00000000000001&nbsp;51,&nbsp;32.39018064403226&nbsp;50.96157056080646,&nbsp;32.76536686473018&nbsp;50.84775906502257,&nbsp;33.11114046603921&nbsp;50.66293922460509,&nbsp;33.4142135623731&nbsp;50.41421356237309,&nbsp;33.6629392246051&nbsp;50.111140466039195,&nbsp;33.84775906502258&nbsp;49.765366864730176,&nbsp;33.961570560806464&nbsp;49.39018064403225,&nbsp;34&nbsp;49)) |
| 4 | POINT&nbsp;(30.5167&nbsp;50.4333) | POLYGON&nbsp;((32.5167&nbsp;50.4333,&nbsp;32.478270560806465&nbsp;50.04311935596775,&nbsp;32.36445906502257&nbsp;49.66793313526982,&nbsp;32.17963922460509&nbsp;49.3221595339608,&nbsp;31.930913562373096&nbsp;49.01908643762691,&nbsp;31.627840466039206&nbsp;48.77036077539491,&nbsp;31.28206686473018&nbsp;48.58554093497743,&nbsp;30.906880644032256&nbsp;48.47172943919354,&nbsp;30.5167&nbsp;48.4333,&nbsp;30.126519355967744&nbsp;48.47172943919354,&nbsp;29.75133313526982&nbsp;48.58554093497743,&nbsp;29.405559533960798&nbsp;48.77036077539491,&nbsp;29.102486437626904&nbsp;49.01908643762691,&nbsp;28.85376077539491&nbsp;49.3221595339608,&nbsp;28.668940934977428&nbsp;49.66793313526983,&nbsp;28.55512943919354&nbsp;50.04311935596775,&nbsp;28.5167&nbsp;50.4333,&nbsp;28.55512943919354&nbsp;50.82348064403226,&nbsp;28.668940934977428&nbsp;51.198666864730185,&nbsp;28.85376077539491&nbsp;51.54444046603921,&nbsp;29.102486437626908&nbsp;51.8475135623731,&nbsp;29.405559533960798&nbsp;52.09623922460509,&nbsp;29.751333135269824&nbsp;52.281059065022575,&nbsp;30.126519355967748&nbsp;52.39487056080647,&nbsp;30.516700000000004&nbsp;52.4333,&nbsp;30.906880644032263&nbsp;52.39487056080646,&nbsp;31.282066864730186&nbsp;52.281059065022575,&nbsp;31.62784046603921&nbsp;52.09623922460509,&nbsp;31.9309135623731&nbsp;51.847513562373095,&nbsp;32.1796392246051&nbsp;51.5444404660392,&nbsp;32.36445906502258&nbsp;51.19866686473018,&nbsp;32.478270560806465&nbsp;50.82348064403225,&nbsp;32.5167&nbsp;50.4333)) |
| 5 | POINT&nbsp;(30.5167&nbsp;50.4333) | POLYGON&nbsp;((32.5167&nbsp;50.4333,&nbsp;32.478270560806465&nbsp;50.04311935596775,&nbsp;32.36445906502257&nbsp;49.66793313526982,&nbsp;32.17963922460509&nbsp;49.3221595339608,&nbsp;31.930913562373096&nbsp;49.01908643762691,&nbsp;31.627840466039206&nbsp;48.77036077539491,&nbsp;31.28206686473018&nbsp;48.58554093497743,&nbsp;30.906880644032256&nbsp;48.47172943919354,&nbsp;30.5167&nbsp;48.4333,&nbsp;30.126519355967744&nbsp;48.47172943919354,&nbsp;29.75133313526982&nbsp;48.58554093497743,&nbsp;29.405559533960798&nbsp;48.77036077539491,&nbsp;29.102486437626904&nbsp;49.01908643762691,&nbsp;28.85376077539491&nbsp;49.3221595339608,&nbsp;28.668940934977428&nbsp;49.66793313526983,&nbsp;28.55512943919354&nbsp;50.04311935596775,&nbsp;28.5167&nbsp;50.4333,&nbsp;28.55512943919354&nbsp;50.82348064403226,&nbsp;28.668940934977428&nbsp;51.198666864730185,&nbsp;28.85376077539491&nbsp;51.54444046603921,&nbsp;29.102486437626908&nbsp;51.8475135623731,&nbsp;29.405559533960798&nbsp;52.09623922460509,&nbsp;29.751333135269824&nbsp;52.281059065022575,&nbsp;30.126519355967748&nbsp;52.39487056080647,&nbsp;30.516700000000004&nbsp;52.4333,&nbsp;30.906880644032263&nbsp;52.39487056080646,&nbsp;31.282066864730186&nbsp;52.281059065022575,&nbsp;31.62784046603921&nbsp;52.09623922460509,&nbsp;31.9309135623731&nbsp;51.847513562373095,&nbsp;32.1796392246051&nbsp;51.5444404660392,&nbsp;32.36445906502258&nbsp;51.19866686473018,&nbsp;32.478270560806465&nbsp;50.82348064403225,&nbsp;32.5167&nbsp;50.4333)) |
