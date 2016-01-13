# Analysis of GDELT using GeoMesa

This sample project demonstrates how to analyze the Global Database of Events, Language, and Tone
(GDELT; http://www.gdeltproject.org) using GeoMesa.

To ingest:
```
mvn install
hadoop jar target/geomesa-gdelt-1.0-SNAPSHOT.jar com.example.geomesa.gdelt.GDELTIngest -instanceId <instanceId> -zookeepers <zookeepers> -user <user> -password <password> -auths <auths> -tableName <tableName> -featureName <featureName> -ingestFile <ingestFile>
```

It will copy your jar to HDFS and requires that the ingestFile be a GDELT format TSV that is already on HDFS.

Caveats:
1) This code is meant for ingesting the full-resolution GDELT data files, NOT the "Reduced" data that has fewer columns.
2) The data to ingest should be in tab-separated value (TSV) format.  Even though the full-resolution GDELT data
files have a .CSV extension, they use tabs for separators, not commas.

If you have any questions, please email the GeoMesa user's list at geomesa-users@locationtech.org
