package com.jentest.germanRentals.rdd

import com.jentest.germanRentals.model.{MinsAndMaxes, ReducedImmo}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object RDDBasedManipulator extends LazyLogging {

  def nullSafeGetLong(row: Row, i: Int) = {
    if (row.isNullAt(i)) 0L else row.getLong(i)
  }

  def nullSafeGetDouble(row: Row, i: Int) = {
    if (row.isNullAt(i)) 0.0 else row.getDouble(i)
  }

  def nullSafeGetString(row: Row, i: Int) = {
    if (row.isNullAt(i)) "" else row.getString(i)
  }

  def getWordArray(description: String): Map[String, Int] = {
    description.split(" ")
      .groupBy(identity _)
      .mapValues(_.size)
      .map(identity)
  }

  def getTotalWordCount(wordMap: Map[String, Int]): Int = {
    wordMap
      .values
      .reduce((total, cur) => total + cur)
  }

  def analyzeData(sparkSession: SparkSession, dataDirectory: String): RDD[MinsAndMaxes] = {

    val areas: RDD[Area] = AreaExtractor
      .getStatesWithPostalCodes(dataDirectory + "/geoData", sparkSession)
      .distinct

    val houses: RDD[House] = HouseExtractor.getHouseSales(dataDirectory + "/houses", sparkSession)

    val groupedAreas = areas.groupBy(area => area.postalCode)
    val groupedHouses = houses.groupBy(house => house.plz)
    val housesWithArea = groupedHouses.join(groupedAreas)
    housesWithArea.take(5).foreach(println)

    val immoDF = sparkSession
      .read
      .parquet(dataDirectory + "/wohnung")

    val immoRDD = immoDF.rdd

    /**
     * scoutId = 12 long
     * pricetrend = 8 double
     * totalRent = 10 double
     * serviceCharge =  1 double
     * baseRent = 19 double
     * description = 41 string
     */

    val descriptionWordsAndRegions = immoRDD.map((row: Row) => {
      val description = nullSafeGetString(row, 41)
      val wordArray = getWordArray(description)
      val wordCount = getTotalWordCount(wordArray)
      val totalRent = nullSafeGetDouble(row, 10)
      val baseRent = nullSafeGetDouble(row, 19)
      val serviceCharge = nullSafeGetDouble(row, 1)
      val cleansedRent = List(totalRent, baseRent, serviceCharge).max
      ReducedImmo(
        nullSafeGetLong(row, 12),
        nullSafeGetDouble(row, 8),
        cleansedRent,
        totalRent,
        serviceCharge,
        baseRent,
        description,
        wordArray,
        wordCount,
        nullSafeGetString(row, 0),
        nullSafeGetString(row, 39),
        nullSafeGetString(row, 40)
      )
    })

    val villas: RDD[ReducedImmo] = descriptionWordsAndRegions.filter(row => {
      row.wordMap.keys.toArray.contains("Villa")
    })

    /*    descriptionWordsAndRegions.take(5).foreach(rImmo => {
          println(s"scoutId: ${rImmo.scoutId}"
            + s"\tpriceTrend: ${rImmo.pricetrend}"
            + s"\ttotalRent: ${rImmo.totalRent}\tserviceCharge: ${rImmo.serviceCharge}"
            + s"\tbaseRent: ${rImmo.baseRent}\n"
            + s"${rImmo.wordMap}\ntotalWordCount: ${rImmo.totalWordCount}")
        })*/

    sparkSession.sparkContext.emptyRDD[MinsAndMaxes]
  }

}
