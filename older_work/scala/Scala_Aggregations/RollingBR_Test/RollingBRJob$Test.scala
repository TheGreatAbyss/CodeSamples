package com.company.air.RollingBR

import com.company.air.RollingBR.BaseObjects.EVENT_1Key
import com.company.air.RollingBR.RollingBRJob._
import com.company.air.RollingBR.TestBuilders._
import com.company.air.TestBuilders._
import com.company.air.{Params, S3LoaderParams, SparkJobSpec}
import com.company.data.EVENT_1Data
import com.company.util.DateUtils._
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.scalatest.FunSuite


class RollingBRJob$Test extends FunSuite with SparkJobSpec {

  val user = System.getenv("USER")

  test("builds correct output path") {
    val actual = buildOutputPath("s3a://company-air/booker_rate/output", parseDate("2015-01-01"))
    println(actual)
    assert( actual == "s3a://company-air/booker_rate/output/2015-01-01/")
  }

  test("builds an input path glob with multiple filters") {
    val s3Params = S3LoaderParams("EVENT_1", 4, saleType = List("SUB_ENTITY_1", "SUB_ENTITY_2"), product = List("PRODUCT_1"), publisher = List("BOOKIT","LASTMINUTE_DOT_COM"))
    val actual = inputPathGlob("EVENT_1", "/base/path", parseDate("2015-01-01"), 3, s3Params.saleType, s3Params.product, s3Params.publisher)
    println(actual)
    val expected = "/base/path/EVENT_1/{20141231,20141230,20141229}/{SUB_ENTITY_1,SUB_ENTITY_2}/{PRODUCT_1}/{BOOKIT,LASTMINUTE_DOT_COM}/*"
    println(expected)
    assert( actual == expected)
  }

  test("builds an input path glob with no publisher filter") {
    val s3Params = S3LoaderParams("EVENT_1", 4, saleType = List("SUB_ENTITY_1", "SUB_ENTITY_2"), product = List("PRODUCT_1"))
    val actual = inputPathGlob("EVENT_1", "/base/path", parseDate("2015-01-01"), 3, s3Params.saleType, s3Params.product, s3Params.publisher)
    println(actual)
    val expected = "/base/path/EVENT_1/{20141231,20141230,20141229}/{SUB_ENTITY_1,SUB_ENTITY_2}/{PRODUCT_1}/{*}/*"
    println(expected)
    assert( actual == expected)
  }

  test("builds an input path glob with no product filter") {
    val s3Params = S3LoaderParams("EVENT_1", 4, saleType = List("SUB_ENTITY_1", "SUB_ENTITY_2"), publisher = List("BOOKIT","LASTMINUTE_DOT_COM"))
    val actual = inputPathGlob("EVENT_1", "/base/path", parseDate("2015-01-01"), 3, s3Params.saleType, s3Params.product, s3Params.publisher)
    println(actual)
    val expected = "/base/path/EVENT_1/{20141231,20141230,20141229}/{SUB_ENTITY_1,SUB_ENTITY_2}/{*}/{BOOKIT,LASTMINUTE_DOT_COM}/*"
    println(expected)
    assert( actual == expected)
  }
  
  test("loader function") {
    val rawRDD = s3dataLoader(sc, Params("src/test/resources", "n/a", parseDate("2015-10-07"),3))(S3LoaderParams("EVENT_1", 3, List("*"), List("*"), List("*")))
    assert(rawRDD.count() > 0)
  }

  test("Load only reportable Ad Calls") {
    val expectedID_2: Integer = 100
    val reportableEVENT_1 : EVENT_1Data = testEVENT_1Data(reportable = true, ID_2 = expectedID_2)
    val nonReportableEVENT_1 = testEVENT_1Data(reportable = false)
    val dontCare: Int = 5

    def testLoader(z: S3LoaderParams): RDD[String] = {
      sc.makeRDD(List(nonReportableEVENT_1.toTypedData.toJson, reportableEVENT_1.toTypedData.toJson))
    }
    val EVENT_1s: RDD[(EVENT_1Key, DateTime)] = loadEVENT_1s(testLoader,dontCare)
    assert(EVENT_1s.count() == 1)
    assert(EVENT_1s.first()._1.ID_2 == expectedID_2)
  }

  test("Reduce to MinEVENT_1Key") {
    val d1 = new DateTime(2015,1,1,12,5,45,21)
    val d2 = new DateTime(2015,1,1,12,5,45,45)
    val d3 = new DateTime(2015,1,1,12,5,45,21)
    val d4 = new DateTime(2015,1,1,11,5,45,21)

    val testRDD = sc.makeRDD(List(buildMinEVENT_1KeyRequestedAtTuple("PC1", d1), buildMinEVENT_1KeyRequestedAtTuple("PC1", d2),
      buildMinEVENT_1KeyRequestedAtTuple("PC2", d3), buildMinEVENT_1KeyRequestedAtTuple("PC2", d4)))
    val result: Array[(EVENT_1Key, DateTime)] = testRDD.reduceByKey((a,b) => if (a.isBefore(b)) a else b).collect()

    assert(result.length == 2)

    val(p1, minDt1) = result(0)
    val(p2, minDt2) = result(1)

    def doMatch(key:EVENT_1Key, dt:DateTime):String = {
      (key.productCategoryType, dt) match {
        case ("PC1", d1) => "correct"
        case ("PC2", d2) => "correct"
        case _ => "fail"
      }
    }

    assert(doMatch(p1, minDt1) == "correct")
    assert(doMatch(p2, minDt2) == "correct")

  }

  test("Eric's Helper") {
    val loaderFunction = s3dataLoader(sc, Params("src/test/resources", "n/a", DateTime.parse("2015-10-07"),3))_
    val EVENT_1sAll = loadEVENT_1s(loaderFunction, LOOK_BACK_DAYS )
    val minEVENT_1Key: RDD[(EVENT_1Key, DateTime)] = EVENT_1sAll.reduceByKey((a,b) => if (a.isBefore(b)) a else b)
    val someMinEVENT_1Key = minEVENT_1Key.take(5)
    someMinEVENT_1Key.foreach(a => println(a._1.ID_1, a._2))
  }

}