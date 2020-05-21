package com.jentest.germanRentals

import com.jentest.germanRentals.dataframe.DataframeBasedManipulator
import com.jentest.germanRentals.model.ReducedImmo
import com.jentest.germanRentals.rdd.RDDBasedManipulator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import scopt.OptionParser

case class Config(apiToUse: String = "ds")

object Main {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder()
      .appName("deutscheWohnung")
      .master("local[4]")
      .getOrCreate()

    val parser = new OptionParser[Config]("test config") {
      opt[String]('a', "api")
        .optional()
        .action((x, c) => c.copy(apiToUse = x))
        .text("This config item is optional.  It specifies whether to use ds or rdd.  'ds' is default.  To use the rdd api, pass in rdd as a value.")
    }

    val rdd: Boolean = parser.parse(args, Config()) match {
      case Some(config) =>
        if (config.apiToUse.equalsIgnoreCase("rdd")) {
          true
        }
        else {
          false
        }
      case _ => false
    }

    var rddOut: RDD[ReducedImmo] = sparkSession.sparkContext.emptyRDD[ReducedImmo]
    var dfOut: DataFrame = sparkSession.emptyDataFrame


    if (rdd) {
      rddOut = RDDBasedManipulator.analyzeData(sparkSession)
      println(rddOut.count)
    }
    else {
      dfOut = DataframeBasedManipulator.analyzeData(sparkSession)
      println(dfOut.count)
    }

    sparkSession.stop
  }

}
