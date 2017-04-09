package com.company.air.RollingBR

import com.company.air.RollingBR.BaseObjects.{AdCall, event_2, _}
import com.company.air._
import com.company.util.{EmrJobConfig, FromJson}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

object RollingBRJob extends WorkFlow with S3Loader {

  val LOOK_BACK_DAYS = 1

  def main (args: Array[String]) {

    val sc = getSC()
    val params= new CommandLineOptionParser().parseOptions(args)

    println("Starting Job")
    runWorkflow(sc, params)
    println("Job Finished")

  }

  @Override
  def runWorkflow(sc: SparkContext, params:Params):Unit = {
    val loaderFunction = s3dataLoader(sc, params)_

    val adCallsAll = loadAdCalls(loaderFunction, LOOK_BACK_DAYS )
    val user_min_event_1 = adCallsAll.reduceByKey((a,b) => if (a.isBefore(b)) a else b)

    val event_2sAll = loadevent_2s(loaderFunction, LOOK_BACK_DAYS)
//    val user_max_event_2 = event_2sAll.reduceByKey((a,b) => if(a))



    println(adCallsAll.take(10))
    println("Got through variables, starting save")
//    saveAdCalls(sc, params.baseOutputPath, params.runDate, adCallsAll)
    println("Save Complete")
  }

  def loadAdCalls(s3loaderFunction:S3LoaderParams => RDD[String], lookBackDays:Int):RDD[(AdCallKey, DateTime)] = {
    s3loaderFunction(S3LoaderParams("event_1", lookBackDays, saleType = List("SUB_ENTITY"), product = List("PRODUCT_1"),  publisher = List("SOME_COMPANY")))
      .map(json => AdCall(FromJson.adCall(json)))
      .filter(a => a.isReportable)
      .map(a => (AdCallKey(a), a.requestedAt))
  }

  def buildOutputPath(baseOutputPath: String, runDate: DateTime): String = {
    baseOutputPath +"/" + runDate.toString("YYYY-MM-dd") + "/"
  }

  def saveAdCalls(sc: SparkContext, baseOutputPath: String, runDate: DateTime, adCalls: RDD[AdCall]): Unit = {
    val outputPath = buildOutputPath(baseOutputPath, runDate)
    println(s"Save ad Calls")
    adCalls.coalesce(25).saveAsTextFile(outputPath)
    println("Done saving")
  }

  def loadevent_2s(s3loaderFunction:S3LoaderParams => RDD[String], lookBackDays:Int):RDD[(event_2Key, DateTime)] = {
    s3loaderFunction(S3LoaderParams("event_2", lookBackDays, saleType = List("SUB_ENTITY"), product = List("NO_AD_TYPE"), publisher = List("SOME_COMPANY")))
    .map(json => event_2(FromJson.event_2(json)))
    .filter(c => c.isReportable)
    .map(c => (event_2Key(c), c.requestedAt))
  }

  override def getEmrConfig(): EmrJobConfig = EmrJobConfig(masterInstanceType = "m3.xlarge", slaveInstanceType = "c3.4xlarge",
  mainClass = this.getClass.getName, driverMemory = "20G", instanceCount = 3, numExecutors = 2,
  executorMemory = "15g", executorCores = 16, baseOutputPath = "s3a://company-air/booker_rate/output", 
  emrName = "Test Scala EMR SDK | EA | Booking Rate" )

//
//  case class EmrJobConfig(masterInstanceType:String, slaveInstanceType:String, mainClass:String,
//                          driverMemory: String, numExecutors:Int, executorMemory:String, executorCores:Int,
//                          baseOutputPath: String, EmrName: String)

  override def getLoadableConfig(): Option[LoadableConfig] = None
}
