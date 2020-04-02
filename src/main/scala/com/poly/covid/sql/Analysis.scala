package com.poly.covid.sql

import com.poly.Utils._
import com.poly.covid.utility._
import org.apache.commons.lang3.time.StopWatch
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel


class Analysis extends java.io.Serializable {

  val config: ConfigProps = new ConfigProps
  val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val date = format.format(new java.util.Date())
  val sw = new StopWatch


  def run(sparkSession: SparkSession, sc: SparkContext, sql: SQLContext, stringBuilder: java.lang.StringBuilder): Unit = {
    sw.start()
    val utils: SparkUtils = new SparkUtils(sc, stringBuilder)
    val schemas: Schemas = new Schemas

    try {
      /*imports functions*/
      /**/ val jhu = sql
        .read
        .schema(schemas.jhu())
        .csv("s3a://poly-testing/covid/jhu/transformed/*")
        .distinct()
      jhu.createOrReplaceTempView("jhuv")

      val cds = sql
        .read
        .schema(schemas.cds())
        .option("header", "true")
        .csv("s3a://poly-testing/covid/cds/*")
        .distinct()
      cds.createOrReplaceTempView("cdsv")

      sparkSession.sql(
        """
          select distinct
          city,
          county,
          state,
          country,
          population,
          --round(Latitude * 2,2)/2 Latitude,
          --round(Longitude * 2,2)/2 Longitude,
          Latitude,
          Longitude,
          url,
          aggregate,
          timezone,
          cases,
          deaths,
          recovered,
          active,
          tested,
          growthFactor,
          Last_Update
          from cdsv
          --where country = 'USA' --and state = 'Normandie' --and county = 'Seminole County'
          """.stripMargin
      ).persist(StorageLevel.MEMORY_ONLY_SER)
        .createOrReplaceTempView("cds")


      sparkSession.sql(
        """
          select distinct
          fips,
          case when Country_Region = 'US' then admin || ' County' else admin end as county,
          Province_State,
          Country_Region,
          Last_Update,
          --round(Latitude * 2,2)/2 Latitude,
          --round(Longitude * 2,2)/2 Longitude,
          Latitude,
          Longitude,
          Confirmed,
          Deaths,
          Recovered,
          Active,
          Combined_Key
          from jhuv
          --where Country_Region = 'US' --and Province_State = 'Normandie' --and admin = 'Seminole'
          """.stripMargin
      ).persist(StorageLevel.MEMORY_ONLY_SER)
        .createOrReplaceTempView("jhu")

      sparkSession.sql(
        """
                select distinct
                a.city,
                a.county,
                a.state,
                a.country,
                a.population,
                a.Latitude,
                a.Longitude,
                a.url,
                a.aggregate,
                a.timezone,
                a.cases,
                b.confirmed as US_Confirmed_County,
                a.deaths,
                b.deaths as US_Deaths_County,
                a.recovered,
                b.recovered as US_Recovered_County,
                a.active,
                b.active as US_Active_County,
                a.tested,
                a.growthFactor,
                a.Last_Update
                from cds a left join jhu b on a.Last_Update = b.Last_Update and b.county = a.county
                where country != 'USA'
                order by 1 DESC
                """.stripMargin
      ).show(100, false)


      sw.stop()
    }
    catch {
      case e: Exception => {
        println("Exception", e)
        stringBuilder.append("ERROR " + e.getMessage).append("\n")
      }
    }
  }
}

