package com.poly.covid.utility

import org.apache.spark.sql.types._


class Schemas extends Serializable {

  def jhu(): StructType = {
    StructType(Seq(
      StructField("fips", StringType, true),
      StructField("admin", StringType, true),
      StructField("Province_State", StringType, true),
      StructField("Country_Region", StringType, true),
      StructField("Last_Update", DateType, true),
      StructField("Latitude", DoubleType, true),
      StructField("Longitude", DoubleType, true),
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
      StructField("name", StringType, true),
      StructField("level", StringType, true),
      StructField("city", StringType, true),
      StructField("county", StringType, true),
      StructField("state", StringType, true),
      StructField("country", StringType, true),
      StructField("population", StringType, true),
      StructField("Latitude", DoubleType, true),
      StructField("Longitude", DoubleType, true),
      StructField("url", StringType, true),
      StructField("aggregate", StringType, true),
      StructField("timezone", StringType, true),
      StructField("cases", StringType, true),
      StructField("deaths", StringType, true),
      StructField("recovered", StringType, true),
      StructField("active", StringType, true),
      StructField("tested", StringType, true),
      StructField("growthFactor", StringType, true),
      StructField("Last_Update", DateType, true)
    )
    )
  }

  def covidStruct(): StructType = {
    StructType(Seq(
      StructField("name", StringType, true),
      StructField("level", StringType, true),
      StructField("city", StringType, true),
      StructField("county", StringType, true),
      StructField("state", StringType, true),
      StructField("country", StringType, true),
      StructField("population", StringType, true),
      StructField("latitude", DoubleType, true),
      StructField("longitude", DoubleType, true),
      StructField("url", StringType, true),
      StructField("aggregate", StringType, true),
      StructField("timezone", StringType, true),
      StructField("cases", StringType, true),
      StructField("us_confirmed_county", StringType, true),
      StructField("deaths", StringType, true),
      StructField("us_deaths_county", StringType, true),
      StructField("recovered", StringType, true),
      StructField("us_recovered_county", StringType, true),
      StructField("active", StringType, true),
      StructField("us_active_county", StringType, true),
      StructField("tested", StringType, true),
      StructField("growth_factor", StringType, true),
      StructField("last_updated", DateType, true)
    )
    )
  }


}
