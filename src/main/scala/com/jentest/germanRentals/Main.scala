package com.jentest.germanRentals

import com.jentest.germanRentals.dataframe.DataframeBasedManipulator
import com.jentest.germanRentals.model.ReducedImmo
import com.jentest.germanRentals.rdd.RDDBasedManipulator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import scopt.OptionParser

case class Config(
                   apiToUse: String = "ds",
                   local: Boolean = true,
                   dataDirectory: String = "/data")

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
        .text("This config item is optional." +
          "  It specifies whether to use ds or rdd.  'ds' is default." +
          "  To use the rdd api, pass in rdd as a value.")

      opt[Boolean]('l', "local")
        .optional()
        .action((y, c) => c.copy(local = y))
        .text("This config item is optional." +
          "  If false, we are running on an emr cluster.")

      opt[String]('d', "dataDir")
        .action((d, c) => c.copy(dataDirectory = d))
        .text("Prefix for defining location of where" +
          " the data files live.  May be a local directory or an s3 bucket.")
    }

    val jobConfig: Option[Config] = parser.parse(args, Config())

    val rdd: Boolean = jobConfig.get.apiToUse.equalsIgnoreCase("rdd")

    var rddOut: RDD[ReducedImmo] = sparkSession.sparkContext.emptyRDD[ReducedImmo]
    var dfOut: DataFrame = sparkSession.emptyDataFrame


    if (rdd) {
      rddOut = RDDBasedManipulator.analyzeData(sparkSession, jobConfig.get.dataDirectory)
      println(rddOut.count)
    }
    else {
      dfOut = DataframeBasedManipulator.analyzeData(sparkSession, jobConfig.get.dataDirectory)
      println(dfOut.count)
    }

    sparkSession.stop
  }

}
