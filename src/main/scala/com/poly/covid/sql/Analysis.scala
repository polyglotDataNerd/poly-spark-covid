package com.poly.covid.sql

import java.util.concurrent.TimeUnit

import com.poly.Utils._
import com.poly.covid.utility._
import org.apache.commons.lang3.time.{DateUtils, StopWatch}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel


class Analysis extends java.io.Serializable {

  val config: ConfigProps = new ConfigProps
  val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val date = format.format(DateUtils.addDays(new java.util.Date(), -0))
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
      println(date + ": jhu count: " + jhu.count())

      /* only takes current day pull and not all files since the
      go pipeline takes current and history daily */
      val cds = sql
        .read
        .option("quote", "\"")
        .option("escape", "\"")
        .schema(schemas.cds())
        .option("header", "true")
        .csv("s3a://poly-testing/covid/cds/" + date + "/*")
        .distinct()
      cds.createOrReplaceTempView("cdsv")
      println(date + ": cds count: " + cds.count())

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
          sum(cases) cases,
          sum(deaths) deaths,
          sum(recovered) recovered,
          sum(active) active,
          sum(tested) tested,
          sum(hospitalized) hospitalized,
          sum(discharged) discharged,
          max(growthFactor) growthFactor,
          Last_Update
          from cdsv
          group by
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
            Last_Update
          """.stripMargin
      ).persist(StorageLevel.MEMORY_ONLY_SER)
        .createOrReplaceTempView("cds")
      sparkSession.sql("""select max(cast(Last_Update as date)) latest_update_cds from cds""").show(1, false)


      sparkSession.sql(
        """
          select distinct
          fips,
          admin as county,
          Province_State,
          Country_Region,
          Last_Update,
          Latitude,
          Longitude,
          sum(Confirmed) Confirmed,
          sum(Deaths) Deaths,
          sum(Recovered) Recovered,
          sum(Active) Active,
          Combined_Key
          from jhuv
          group by
            fips,
            admin,
            Province_State,
            Country_Region,
            Last_Update,
            Latitude,
            Longitude,
            Combined_Key
          """.stripMargin
      ).persist(StorageLevel.MEMORY_ONLY_SER)
        .createOrReplaceTempView("jhu")
      sparkSession.sql("""select max(cast(Last_Update as date)) latest_update_jhu from jhu""").show(1, false)


      /* denormalized table is exploded so will have possible duplicity overwrites since it consolidates history/current daily */
      val combined = sparkSession.sql(
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
                a.cases,
                b.Confirmed as US_Confirmed_County,
                a.deaths,
                b.Deaths as US_Deaths_County,
                a.recovered,
                b.Recovered as US_Recovered_County,
                a.active,
                b.Active as US_Active_County,
                tested,
                a.hospitalized,
                a.discharged,
                a.growthFactor,
                a.Last_Update
           from cds a left join jhu b
             on a.Last_Update = b.Last_Update
             and lower(trim(substring_index(a.county, ' ', 1))) = lower(trim(b.county))
             and lower(trim(a.state)) = lower(trim(b.Province_State))
           order by country DESC, city ASC
                """.stripMargin
      )
      combined.createOrReplaceTempView("combined")
      println(date + ": combined count: " + combined.count())
      sparkSession.sql("""select max(cast(Last_Update as date)) latest_update_combined from combined""").show(1, false)

      utils.gzipWriter("s3a://poly-testing/covid/combined/", combined)
      sw.stop()
      println("INFO spark process runtime (seconds): " + sw.getTime(TimeUnit.SECONDS))

      /* QA */
      new CovidQA().runQA(sparkSession, sql, stringBuilder)
    }
    catch {
      case e: Exception => {
        println("Exception", e)
        stringBuilder.append("ERROR " + e.getMessage).append("\n")
      }
    }
  }
}

