package com.jentest.germanRentals.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import RDDBasedManipulator.{nullSafeGetString, nullSafeGetLong, nullSafeGetDouble}

case class Area(postalCode: Long, state: String)

object AreaExtractor {

  def getStatesWithPostalCodes(geoDataFileSource: String, spark: SparkSession): RDD[Area] = {
    val geoDataRDD: RDD[Row] = spark
      .read
      .parquet(geoDataFileSource)
      .rdd

    geoDataRDD.map((areaRow: Row) => {
      Area(
        nullSafeGetLong(areaRow, 1),
        nullSafeGetString(areaRow, 4)
      )
    })
  }
}
