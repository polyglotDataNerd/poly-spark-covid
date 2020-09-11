package com.poly.covid.sql

import java.net.URI
import java.util.concurrent.TimeUnit

import com.poly.covid.utility._
import com.poly.utils._
import org.apache.commons.lang3.time.{DateUtils, StopWatch}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel


class Analysis extends java.io.Serializable {

  val config: ConfigProps = new ConfigProps
  val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val date = format.format(DateUtils.addDays(new java.util.Date(), -0))
  val   sw = new StopWatch


  def run(sc: SparkContext, sqlContext: SQLContext, stringBuilder: java.lang.StringBuffer): Unit = {
    sc.setCheckpointDir("/tmp/checkpoints")
    sw.start()
    lazy val utils: SparkUtils = new SparkUtils(sc, stringBuilder)
    lazy val schemas: Schemas = new Schemas

    try {
      /* write src to ORC */
      utils.orcWriterSnappy("s3a://poly-testing/covid/orc/jhu/", sqlContext
        .read
        .schema(schemas.jhu())
        .csv("s3a://poly-testing/covid/jhu/transformed/*")
        .distinct())

      utils.orcWriterSnappy("s3a://poly-testing/covid/orc/cds/", sqlContext
        .read
        .option("quote", "\"")
        .option("escape", "\"")
        .schema(schemas.cdsNew())
        .option("header", "true")
        .csv("s3a://poly-testing/covid/cds/" + date + "/*")
        .distinct())

      /* uses ORC source to write combined dataset */
      val jhu = sc.broadcast(sqlContext
        .read
        .orc("s3a://poly-testing/covid/orc/jhu/*")
      )
      jhu.value.persist(StorageLevel.MEMORY_ONLY_SER_2).createOrReplaceTempView("jhuv")

      /* only takes current day pull and not all files since the
      go pipeline takes current and history daily */
      val cds = sc.broadcast(sqlContext
        .read
        .orc("s3a://poly-testing/covid/orc/cds/*")
      )
      cds.value.persist(StorageLevel.MEMORY_ONLY_SER_2).createOrReplaceTempView("cdsv")

      sqlContext.sql(
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
          --url,
          aggregate,
          timezone,
          icu,
          hospitalized_current,
          icu_current,
          sum(cases) cases,
          sum(deaths) deaths,
          sum(recovered) recovered,
          sum(active) active,
          sum(tested) tested,
          sum(hospitalized) hospitalized,
          sum(discharged) discharged,
          --max(growthFactor) growthFactor,
          last_updated
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
            --url,
            aggregate,
            timezone,
            icu,
            last_updated,
            hospitalized_current,
            icu_current
          """.stripMargin
      ).persist(StorageLevel.MEMORY_AND_DISK_SER_2)
        .createOrReplaceTempView("cds")

      sqlContext.sql(
        """
          select distinct
          fips,
          admin as county,
          Province_State,
          Country_Region,
          last_updated,
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
            last_updated,
            Latitude,
            Longitude,
            Combined_Key
          """.stripMargin
      ).persist(StorageLevel.MEMORY_AND_DISK_SER_2)
        .createOrReplaceTempView("jhu")

      /* denormalized table is exploded so will have possible duplicity overwrites since it consolidates history/current daily */
      utils.gzipWriter("s3a://poly-testing/covid/combined/", sqlContext.sql(
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
                --a.url,
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
                --a.growthFactor,
                a.last_updated,
                a.icu,
                a.hospitalized_current,
                a.icu_current
           from cds a left join jhu b
             on a.last_updated = b.last_updated
             and lower(trim(substring_index(a.county, ' ', 1))) = lower(trim(b.county))
             and lower(trim(a.state)) = lower(trim(b.Province_State))
           order by country DESC, city ASC
                """.stripMargin
      ))

      /* rename spark output file */
      val fs = FileSystem.get(new URI(s"s3a://poly-testing"), sc.hadoopConfiguration)
      val fileName = fs.globStatus(new Path("s3a://poly-testing/covid/combined/part*"))(0).getPath.getName.trim
      fs.rename(new Path("s3a://poly-testing/covid/combined/" + fileName), new Path("s3a://poly-testing/covid/combined/covid19_combined.gz"))

      /* write to ORC */
      utils.orcWriterSnappy("s3a://poly-testing/covid/orc/combined/", sqlContext
        .read
        .option("header", true)
        .option("delimiter", "\t")
        .option("quote", "\"")
        .option("escape", "\"")
        .schema(schemas.covidStructNew())
        .csv("s3a://poly-testing/covid/combined/*")
        .distinct())

      sw.stop()
      println("INFO spark process runtime (seconds): " + sw.getTime(TimeUnit.SECONDS))

      /* QA */
      new CovidQA().runQA(sc, sqlContext, stringBuilder)
    }
    catch {
      case e: Exception => {
        println("Exception", e)
        stringBuilder.append("ERROR " + e.getMessage).append("\n")
      }
    }
  }
}

