package com.jentest.germanRentals.dataframe

import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DataFrameBasedManipulator {

  def analyzeData(sparkSession: SparkSession, dataDirectory: String): DataFrame = {
    import sparkSession.implicits._

    val geoDataDF = sparkSession
      .read
      .parquet(dataDirectory + "/geoData")

    val areas = geoDataDF
      .select(
        "postalCode",
        "state"
      ).distinct


    val housesDF = sparkSession
      .read
      .parquet(dataDirectory + "/houses")
      .withColumnRenamed("Wohnflaeche__m²_", "Wohnflaeche")
      .withColumnRenamed("Warmmiete__in_€_", "Warmmiete")
      .withColumnRenamed("Garten/_mitnutzung", "Garten")

    val housesStep1 = housesDF
      .select(
        "Haustyp",
        "Zimmer",
        "Wohnflaeche",
        "plz",
        "Baujahr",
        "Preis",
        "Provision",
        "ExactPreis",
        "Terrasse",
        "Garten",
        "Aktuell_vermietet",
        "Haustiere_erlaubt",
        "Warmmiete"
      )

    val houses = housesStep1
      .join(areas, $"plz" === $"postalCode", "left")
      .select(
        $"postalCode",
        $"state",
        col("Warmmiete").alias("rent"),
        greatest($"Preis", $"ExactPreis").alias("purchasePrice"),
        $"Zimmer",
        doHousesHave($"Garten").alias("Garten"),
        doHousesHave($"Terrasse").alias("Terrasse"),
        doHousesHave($"Haustiere_erlaubt").alias("Haustiere")
      )

    val byState = Window.partitionBy('state)

    def augmentDataFrameWithAverageMaxAndMin(dataFrame: DataFrame): DataFrame = dataFrame
      .withColumn("statePurchasePriceAverage", avg('purchasePrice) over byState)
      .withColumn("statePurchasePriceMax", max('purchasePrice) over byState)
      .withColumn("statePurchasePriceMin", min('purchasePrice) over byState)
      .withColumn("stateRentAverage", avg('rent) over byState)
      .withColumn("stateRentMax", max('rent) over byState)
      .withColumn("stateRentMin", min('rent) over byState)

    val homesWithStatewideStatistics = augmentDataFrameWithAverageMaxAndMin(houses)
      .filter('statePurchasePriceMax === 'purchasePrice || 'statePurchasePriceMin === 'purchasePrice)

    val apartmentsDF = sparkSession
      .read
      .parquet(dataDirectory + "/wohnung")

    val wohnungStep1 = apartmentsDF
      .join(areas, $"geo_plz" === $"postalCode", "left")
      .select(
        $"postalCode",
        $"state",
        greatest($"totalRent", $"serviceCharge", $"baseRent").alias("rent"),
        lit(0).alias("purchasePrice"),
        col("noRooms").alias("Zimmer"),
        col("garden").alias("Garten"),
        split($"description", " ").alias("words")
      )

    val wohnungStep2 = wohnungStep1.select(
      $"*",
      isThereATerrace($"words").alias("Terrasse"),
      arePetsAllowed($"words").alias("Haustiere")
    )

    val apartments = wohnungStep2.select(
      $"postalCode",
      $"state",
      $"rent",
      $"purchasePrice",
      $"Zimmer",
      $"Garten",
      $"Terrasse",
      $"Haustiere"
    )

    val apartmentsWithStats = augmentDataFrameWithAverageMaxAndMin(apartments)
      .filter('stateRentMax === 'rent || 'stateRentMin === 'rent)

    homesWithStatewideStatistics.union(apartmentsWithStats)
  }


  val doHousesHave: UserDefinedFunction = udf((value: String) => {
    value match {
      case "true" => true
      case _ => false
    }
  })

  val isThereATerrace: UserDefinedFunction = udf((description: Seq[String]) => {
    description match {
      case null => false
      case _ => deriveFromDescriptionContents(description, "terrasse")
    }
  })

  val arePetsAllowed: UserDefinedFunction = udf((description: Seq[String]) => {
    description match {
      case null => false
      case _ => deriveFromDescriptionContents(description, "haustiere") ||
        deriveFromDescriptionContents(description, "Haustiere")
    }
  })

  def deriveFromDescriptionContents(description: Seq[String], term: String): Boolean = {
    description.contains(term.toLowerCase)
  }

}

