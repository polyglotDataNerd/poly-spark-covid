package com.poly.covid.utility

import org.apache.spark.sql.types._


class Schemas extends Serializable {
  /*need to define MixPanel schemas as structtype arrays as the scala complier gets a java.lang.StackOverflowError because of a
case class limitation on having a max of 22 fields*/

  def jhu(): StructType = {
    StructType(Seq(
      StructField("fips", StringType, true),
      StructField("admin", StringType, true),
      StructField("Province_State", StringType, true),
      StructField("Country_Region", StringType, true),
      StructField("Last_Update", StringType, true),
      StructField("Latitude", StringType, true),
      StructField("Longitude", StringType, true),
      StructField("Confirmed", StringType, true),
      StructField("Deaths", StringType, true),
      StructField("Recovered", StringType, true),
      StructField("Active", StringType, true),
      StructField("Combined_Key", StringType, true)
    )
    )
  }

  def cds(): StructType = {
    StructType(Seq(
      StructField("city", StringType, true),
      StructField("county", StringType, true),
      StructField("state", StringType, true),
      StructField("country", StringType, true),
      StructField("population", StringType, true),
      StructField("Latitude", StringType, true),
      StructField("Longitude", StringType, true),
      StructField("aggregate", StringType, true),
      StructField("timezone", StringType, true),
      StructField("cases", StringType, true),
      StructField("deaths", StringType, true),
      StructField("recovered", StringType, true),
      StructField("active", StringType, true),
      StructField("tested", StringType, true),
      StructField("rewards_started_at", StringType, true),
      StructField("growthFactor", StringType, true),
      StructField("Last_Update", StringType, true)
    )
    )
  }


  def emaildeletes(): StructType = {
    StructType(Seq(
      StructField("email", StringType, true)
    )
    )
  }


}
