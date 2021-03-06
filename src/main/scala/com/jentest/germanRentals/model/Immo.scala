package com.jentest.germanRentals.model

case class ImmoInputs(
                       regio1: String,
                       serviceCharge: String,
                       heatingType: String,
                       telekomTvOffer: String,
                       telekomHybridUploadSpeed: String,
                       newlyConst: Boolean,
                       balcony: Boolean,
                       pictureCount: Int,
                       priceTrend: String,
                       telekomUploadSpeed: String,
                       totalRent: String,
                       yearConstructed: String,
                       scoutId: String,
                       noParkSpaces: String,
                       firingTypes: String,
                       hasKitchen: Boolean,
                       geo_bln: String,
                       cellar: Boolean,
                       yearConstructedRange: String,
                       baseRent: Double,
                       houseNumber: String,
                       livingSpace: Double,
                       geo_krs: String,
                       condition: String,
                       interiorQual: String,
                       petsAllowed: String,
                       street: String,
                       streetPlain: String,
                       lift: Boolean,
                       baseRentRange: Int,
                       typeOfFlat: String,
                       geo_plz: Int,
                       noRooms: Double,
                       thermalChar: String,
                       floor: String,
                       numberOfFloors: String,
                       noRoomsRange: Int,
                       garden: Boolean,
                       livingSpaceRange: Int,
                       regio2: String,
                       regio3: String,
                       description: String,
                       facilities: String,
                       heatingCosts: String,
                       energyEfficiencyClass: String,
                       lastRefurbish: String,
                       electricityBasePrice: String,
                       electricityKwhPrice: String,
                       date: String
                     )

case class ReducedImmo(
                        scoutId: Long,
                        pricetrend: Double,
                        cleansedRent: Double,
                        totalRent: Double,
                        serviceCharge: Double,
                        baseRent: Double,
                        description: String,
                        wordMap: Map[String, Int],
                        totalWordCount: Int,
                        state: String,
                        region2: String,
                        region3: String
                      )

case class MinsAndMaxes(
                         postalCode: Long,
                         state: String,
                         rent: Double,
                         purchasePrice: Double,
                         Zimmer: String,
                         Garten: Boolean,
                         Terrasse: Boolean,
                         Haustiere: Boolean,
                         statePurchasePriceAverage: Double,
                         statePurchasePriceMax: Double,
                         statePurchasePriceMin: Double,
                         stateRentAverage: Double,
                         stateRentMax: Double,
                         stateRentMin: Double
                       )
