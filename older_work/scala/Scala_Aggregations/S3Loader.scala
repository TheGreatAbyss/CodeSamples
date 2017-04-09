package com.company.air

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

trait S3Loader {

  def s3dataLoader(sc: SparkContext, params:Params)
                (s3loaderParams: S3LoaderParams): RDD[String] = {
    val inputPath= inputPathGlob(s3loaderParams.dataType, params.baseInputPath, params.runDate, s3loaderParams.lookbackDays,
                                  s3loaderParams.saleType, s3loaderParams.product, s3loaderParams.field_1)
    println("Load data for inputpath: " + inputPath)
    sc.textFile(inputPath)
  }

  def inputPathGlob(objectType: String, basePath: String, runDate: DateTime, lookbackDays: Int,
                    saleType:List[String],
                    product:List[String],
                    field_1:List[String]):String = {
    val dates = (1 to lookbackDays).map(day =>
      runDate.minusDays(day).toString("YYYYMMdd")
    )

    def folderFilterString(filter:List[String]):String = { "/{" + filter.mkString(",") + "}" }

      basePath + "/" + objectType + "/{" + dates.mkString(",") + "}" + folderFilterString(saleType) +
        folderFilterString(product) + folderFilterString(field_1) + "/*"
  }
}

case class S3LoaderParams(dataType: String,
                          lookbackDays: Int,
                          saleType:List[String] = List("*"),
                          product:List[String] = List("*"),
                          field_1:List[String] = List("*")
                           )
