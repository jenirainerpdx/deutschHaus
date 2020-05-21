package com.jentest.germanRentals.dataframe

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DataframeBasedManipulator {

  def analyzeData(sparkSession: SparkSession, dataDirectory: String): DataFrame = {
    import sparkSession.implicits._

    val geoDataDF = sparkSession
      .read
      .parquet(dataDirectory + "/geoData")

    val areas = geoDataDF
      .select(
        "postalCode",
        "adminDivision",
        "state",
        "stateAbbrev",
        "district",
        "municipalityKey"
      )

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
        $"adminDivision",
        $"state",
        $"district",
        col("Warmmiete").alias("rent"),
        greatest($"Preis", $"ExactPreis").alias("purchasePrice"),
        $"Zimmer",
        doHousesHave($"Garten").alias("Garten"),
        doHousesHave($"Terrasse").alias("Terrasse"),
        doHousesHave($"Haustiere_erlaubt").alias("Haustiere")
      )


    val apartmentsDF = sparkSession
      .read
      .parquet(dataDirectory + "/wohnung")

    val wohnungStep1 = apartmentsDF
      .join(areas, $"geo_plz" === $"postalCode", "left")
      .select(
        $"postalCode",
        $"adminDivision",
        $"state",
        $"district",
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
      $"adminDivision",
      $"state",
      $"district",
      $"rent",
      $"purchasePrice",
      $"Zimmer",
      $"Garten",
      $"Terrasse",
      $"Haustiere"
    )

    val homes = houses.union(apartments)
    homes.show()

    homes
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
