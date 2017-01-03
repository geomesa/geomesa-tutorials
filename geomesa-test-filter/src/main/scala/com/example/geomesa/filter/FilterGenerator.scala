package com.example.geomesa.filter

import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTimeZone, DateTime}
import org.opengis.filter.Filter
import org.scalacheck.Gen

import scala.collection.JavaConversions._

/**
  * Created by mzimmerman on 7/8/16.
  */
class FilterGenerator {
  val minLon = -180
  val maxLon = 180
  val minLat = -90
  val maxLat = 90
  val minDtg = new DateTime("2016-01-01T00:00:00.000Z")
  val maxDtg = new DateTime("2016-07-01T00:00:00.000Z")
  val ff = CommonFactoryFinder.getFilterFactory2
  val dtFormat = ISODateTimeFormat.dateTime()

  def buildRange(minDt: DateTime, maxDt: DateTime): Gen[(DateTime, DateTime)] = {
    val minMillis: Long = minDt.getMillis
    val maxMillis: Long = maxDt.getMillis

    val gen = for {
      x <- Gen.choose(minMillis, maxMillis)
      y <- Gen.choose(minMillis, maxMillis)
      xt = new DateTime(x, DateTimeZone.UTC)
      yt = new DateTime(y, DateTimeZone.UTC)
    } yield if (x < y) (xt, yt) else (yt, xt)

    gen
  }

  def buildRange(min: Double, max: Double): Gen[(Double, Double)] = {
    val gen = for {
      x <- Gen.choose(min, max)
      y <- Gen.choose(min, max)
    } yield if (x < y) (x,y) else (y, x)

    gen
  }

  def genAttr[T](attr: String, value: Gen[T]): Gen[Filter] = {
    val gen = for {
      x <- value
    } yield ff.equals(ff.property(attr), ff.literal(x))
    gen
  }

  def genGeomBBox(property: String): Gen[Filter] = {
    val gen = for {
      (minX, maxX) <- buildRange(minLat, maxLat)
      (minY, maxY) <- buildRange(minLon, maxLon)
    } yield ff.bbox(property, minX, minY, maxX, maxY, "EPSG:4326")

    gen
  }

  def genTimeBetween(attr: String): Gen[Filter] = {
    val p = ff.property(attr)
    val gen = for {
      (t1, t2) <- buildRange(minDtg, maxDtg)
      t1Expr = ff.literal(t1.toString(dtFormat))
      t2Expr = ff.literal(t2.toString(dtFormat))
    } yield ff.between(p, t1Expr, t2Expr)

    gen
  }

  def genAtom = Gen.oneOf(
    genGeomBBox("Where"),
    genTimeBetween("When"),
    genAttr("color", Gen.oneOf("blue", "red", "green", "orange", "yellow", "purple", "black", "white")),
    genAttr("age", Gen.choose(1, 110)),
    genAttr("name", Gen.oneOf("James", "John", "Peter", "Hannah", "Claire", "Gabriel")),
    //genAttr("flag", Gen.oneOf(true, false)),
    genAttr("country", Gen.oneOf("US", "GB", "CH", "NZ", "FR")),
    genAttr("randInt", Gen.choose(0, 100)),
    genAttr("randDouble", Gen.choose(0.0, 1.0)),
    genAttr("score", Gen.choose[Float](0.0f, 5.0f))
  )

  def genNumberOfAtoms = Gen.choose(1,4)

  def genFlatAnd: Gen[Filter] = {
    for {
      n <- genNumberOfAtoms
      l <- Gen.listOfN(n, genAtom)
    } yield if (l.length == 1) l.head else ff.and(l)
  }

  def runSamples[T](gen: Gen[T], n: Int = 50)(thunk: T => Any) = {
    (0 until n).map( _ => gen.sample.map(thunk) )
  }
}

object FilterGenerator {
  def main(args: Array[String]): Unit = {
    val f = new FilterGenerator()
    f.runSamples(f.genFlatAnd, 50)(x => println(ECQL.toCQL(x)))
  }
}

