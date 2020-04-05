package com.poly.covid.sql

import java.util.concurrent.TimeUnit

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
        .option("quote", "\"")
        .option("escape", "\"")
        .schema(schemas.cds())
        .option("header", "true")
        .csv("s3a://poly-testing/covid/cds/*")
        .distinct()
      cds.createOrReplaceTempView("cdsv")

      sparkSession.sql(
        """
          select distinct
          name,
          level,
          city,
          county,
          state,
          country,
          population,
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
          Latitude,
          Longitude,
          Confirmed,
          Deaths,
          Recovered,
          Active,
          Combined_Key
          from jhuv
          """.stripMargin
      ).persist(StorageLevel.MEMORY_ONLY_SER)
        .createOrReplaceTempView("jhu")

      utils.gzipWriter("s3a://poly-testing/covid/combined/" + date,
        sparkSession.sql(
          """
                select distinct
                a.name,
                a.level,
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
                sum(a.cases),
                sum(b.confirmed) as US_Confirmed_County,
                sum(a.deaths),
                sum(b.deaths) as US_Deaths_County,
                sum(a.recovered),
                sum(b.recovered) as US_Recovered_County,
                sum(a.active),
                sum(b.active) as US_Active_County,
                sum(a.tested),
                max(a.growthFactor),
                a.Last_Update
                from cds a left join jhu b on a.Last_Update = b.Last_Update and b.county = a.county
                group by
                a.name,
                a.level,
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
                a.Last_Update
                order by country DESC, city ASC
                """.stripMargin
        ))
      sw.stop()
      println("INFO spark process runtime (seconds): " + sw.getTime(TimeUnit.SECONDS))
    }
    catch {
      case e: Exception => {
        println("Exception", e)
        stringBuilder.append("ERROR " + e.getMessage).append("\n")
      }
    }
  }
}

