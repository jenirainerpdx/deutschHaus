package com.jentest.germanRentals.rdd

import org.apache.spark.sql.{Row, SparkSession}
import RDDBasedManipulator.{nullSafeGetDouble, nullSafeGetLong, nullSafeGetString}
import org.apache.spark.rdd.RDD

case class House(
                  Zimmer: String,
                  Warmmiete: Double,
                  plz: Long,
                  Preis: Double,
                  ExactPreis: Double,
                  Terrasse: Boolean,
                  Garten: Boolean,
                  Haustiere: Boolean
                )

object HouseExtractor {

  def deriveBooleanFromColumnValue(houseRow: Row, i: Int): Boolean = {
    val stringBasedValue = nullSafeGetString(houseRow, i)
    stringBasedValue.toLowerCase match {
      case "true" => true
      case _ => false
    }
  }

  def getHouseSales(houseDataSourceLocation: String, spark: SparkSession): RDD[House] = {
    val houseInputRDD = spark
      .read
      .parquet(houseDataSourceLocation)
      .rdd

    houseInputRDD.map((houseRow: Row) => {
      House(
        nullSafeGetString(houseRow, 4),
        nullSafeGetDouble(houseRow, 38),
        nullSafeGetLong(houseRow, 6),
        nullSafeGetDouble(houseRow, 12),
        nullSafeGetDouble(houseRow, 17),
        deriveBooleanFromColumnValue(houseRow, 26),
        deriveBooleanFromColumnValue(houseRow, 18),
        deriveBooleanFromColumnValue(houseRow, 37)
      )
    })
  }

}
