/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package com.example.geomesa.spark

import java.text.SimpleDateFormat

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.spark.GeoMesaSpark
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

object CountByDay {

  val params = Map(
    "instanceId" -> "mycloud",
    "zookeepers" -> "zoo1,zoo2,zoo3",
    "user"       -> "user",
    "password"   -> "password",
    "tableName"  -> "geomesa.gdelt")

  // see geomesa-tools/conf/sfts/gdelt/reference.conf
  val typeName = "gdelt"
  val geom     = "geom"
  val date     = "dtg"

  val bbox   = "-80, 35, -79, 36"
  val during = "2014-01-01T00:00:00.000Z/2014-01-31T12:00:00.000Z"

  val filter = s"bbox($geom, $bbox) AND $date during $during"

  def main(args: Array[String]) {
    // Get a handle to the data store
    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]

    // Construct a CQL query to filter by bounding box
    val q = new Query(typeName, ECQL.toFilter(filter))

    // Configure Spark
    val conf = new SparkConf().setAppName("testSpark")
    val sc = SparkContext.getOrCreate(conf)

    // Get the appropriate spatial RDD provider
    val spatialRDDProvider = GeoMesaSpark(params)

    // Get an RDD[SimpleFeature] from the spatial RDD provider
    val rdd = spatialRDDProvider.rdd(new Configuration, sc, params, q)

    // Collect the results and print
    countByDay(rdd).collect().foreach(println)
    println("\n")

    ds.dispose()
  }

  def countByDay(rdd: RDD[SimpleFeature], dateField: String = "dtg") = {
    val dayAndFeature = rdd.mapPartitions { iter =>
      val df = new SimpleDateFormat("yyyyMMdd")
      val ff = CommonFactoryFinder.getFilterFactory2
      val exp = ff.property(dateField)
      iter.map { f => (df.format(exp.evaluate(f).asInstanceOf[java.util.Date]), f) }
    }
    dayAndFeature.map( x => (x._1, 1)).reduceByKey(_ + _)
  }
}