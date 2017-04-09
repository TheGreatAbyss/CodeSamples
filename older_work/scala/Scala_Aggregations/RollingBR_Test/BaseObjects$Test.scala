package com.company.air.RollingBR

import com.company.air.RollingBR.BaseObjects._
import com.company.air.SparkJobSpec
import com.company.util.FromJson._
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

/**
 * Created by eric.abis on 10/12/15.
 */
class BaseObjects$Test extends FunSuite with SparkJobSpec {

  val user = System.getenv("USER")
  test("Load test Event_1 data") {
    val rawRDD = sc.textFile("src/test/resources/Event_1/20151006/HOTELS/META/LASTMINUTE_DOT_COM/Event_1-HOTELS-META-LASTMINUTE_DOT_COM-20151006_090001947_i-6d446bc1.log.gz")
    val Event_1RDD: RDD[Event_1] = rawRDD.map(a => Event_1(Event_1(a)))
    val anEvent_1 = Event_1RDD.take(1)(0)
    assert(anEvent_1.productCategoryType == "Hotels")
    assert(anEvent_1.Some_ID == 147)
    assert(anEvent_1.Filterable_Field == true)
    assert(anEvent_1.String_Field_1 == "Unknown")
    assert(anEvent_1.Some_ID_1 == "9eca442c-a554-4eac-b9f9-b169e0c675ff")
    assert(anEvent_1.String_Field_2 == "Unknown")
    assert(anEvent_1.requestedAt.toString == "2015-10-06T07:59:01.021Z")
    assert(anEvent_1.String_Field_3 == "Unknown" )
    assert(anEvent_1.String_Field_4 == "company")
  }



}