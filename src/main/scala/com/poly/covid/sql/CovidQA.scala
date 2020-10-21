package com.poly.covid.sql

import java.util.concurrent.TimeUnit

import com.poly.covid.utility.Schemas
import org.apache.commons.lang3.time.{DateUtils, StopWatch}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{format_number, lead}
import org.apache.spark.storage.StorageLevel

class CovidQA {

  private val sw = new StopWatch
  private lazy val schemas: Schemas = new Schemas
  private val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
  private val date = format.format(DateUtils.addDays(new java.util.Date(), -2))
  private val delim = "\n"

  def runQA(sc: SparkContext, sqlContext: SQLContext, stringBuilder: java.lang.StringBuffer): Unit = {


    sw.start()

    val df = sc.broadcast(sqlContext
      .read
      .schema(schemas.covidStructNew())
      .orc("s3a://poly-testing/covid/orc/combined/*")
      .distinct())
    df.value.persist(StorageLevel.MEMORY_ONLY_SER_2) createOrReplaceTempView ("covid")

    val jhu = sc.broadcast(sqlContext
      .read
      .orc("s3a://poly-testing/covid/orc/jhu/*"))
    jhu.value.persist(StorageLevel.MEMORY_ONLY_SER_2) createOrReplaceTempView ("jhu")

    /* only takes current day pull and not all files since the
    go pipeline takes current and history daily */
    val cds = sc.broadcast(sqlContext
      .read
      .orc("s3a://poly-testing/covid/orc/cds/*")
    )
    cds.value.persist(StorageLevel.MEMORY_ONLY_SER_2).createOrReplaceTempView("cds")


    import sqlContext.implicits._

    stringBuilder.append(String.format("%s", "CDS")).append(delim)
    sqlContext.sql(
      """
        select last_updated as last_updated, count(distinct last_updated||level||county||state||country) as cds_records
        |from cds
        |group by 1
        |order by 1 desc
        |""".stripMargin)
      .persist(StorageLevel.MEMORY_ONLY)
      .take(5)
      .foreach(v => {
        stringBuilder.append(v.mkString("\t")).append(delim)
      })
    stringBuilder.append(delim)

    stringBuilder.append(String.format("%s", "JHU")).append(delim)
    sqlContext.sql(
      """
        select last_updated as last_updated, count(distinct last_updated||Combined_Key||admin||Province_State||Country_Region) as jhu_records
        |from jhu
        |group by 1
        |order by 1 desc
        |""".stripMargin)
      .persist(StorageLevel.MEMORY_ONLY)
      .take(5)
      .foreach(v => {
        stringBuilder.append(v.mkString("\t")).append(delim)
      })
    stringBuilder.append(delim)

    /* matches graph https://coronavirus.jhu.edu/map.html */
    stringBuilder.append(String.format("%s", "US DEATHS")).append(delim)
    sqlContext.sql(
      """
        select
        |last_updated,
        |Country_Region as country,
        |format_number(sum(Deaths), 0) as us_deaths,
        |format_number((sum(Deaths) - LEAD(sum(Deaths), 1) OVER
        |         (PARTITION BY Country_Region ORDER BY last_updated desc)),0) AS dod_deaths,
        |format_number(sum(Confirmed), 0) us_affected,
        |format_number((sum(Confirmed) - LEAD(sum(Confirmed), 1) OVER
        |         (PARTITION BY Country_Region ORDER BY last_updated desc)),0) AS dod_affected
        |from jhu
        where Country_Region = 'US'
        |group by 1,2
        |order by 1 desc
        |""".stripMargin)
      .persist(StorageLevel.MEMORY_ONLY_SER_2)
      .take(10)
      .foreach(v => {
        stringBuilder.append(v.mkString("\t")).append(delim)
      })
    stringBuilder.append(delim)

    stringBuilder.append(String.format("%s", "Combined")).append(delim)
    sqlContext.sql(
      """
        select last_updated, count(distinct last_updated||level||county||state||country) as orc_records
        |from covid
        |group by 1
        |order by 1 desc
        |""".stripMargin)
      .persist(StorageLevel.MEMORY_ONLY)
      .take(5)
      .foreach(v => {
        stringBuilder.append(v.mkString("\t")).append(delim)
      })
    stringBuilder.append(delim)

    stringBuilder.append(String.format("%s", "USA Summary")).append(delim)
    /* all state by day */
    sqlContext.sql(
      """
        |select dbs.last_updated,
        |       dbs.state,
        |       infected,
        |       case when dbs.state != 'New York' then cast(deaths/2 as int) else deaths end as deaths,
        |       format_number(recovered, 0) as recovered,
        |       format_number(hospitalized, 0) as hospitalized,
        |       format_number(discharged, 0) as  discharged
        |from (select last_updated,
        |             state,
        |             cast(sum(deaths) as Integer) deaths
        |      from covid
        |      where country = 'US'
        |      group by 1, 2 ) dbs
        |         join (
        |    select last_updated,
        |           state,
        |           cast(sum(cases) as Integer) infected,
        |           cast(sum(recovered) as Integer)   recovered,
        |           cast(sum(hospitalized) as Integer) hospitalized,
        |           cast(sum(discharged) as Integer)   discharged
        |    from covid
        |    where country = 'US'
        |      and state is not null
        |      and level = 'county'
        |    group by 1, 2) ibs on dbs.state = ibs.state and ibs.last_updated = dbs.last_updated
        |    order by infected desc, deaths desc
        |""".stripMargin)
      .filter($"last_updated" === date)
      .select($"last_updated", $"state", format_number($"infected", 0).as("infected"), format_number($"deaths", 0).as("deaths"), $"recovered", $"hospitalized", $"discharged")
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
      .take(500)
      .foreach(v => {
        stringBuilder.append(v.mkString("\t")).append(delim)
      })
    stringBuilder.append(delim)

    /* state by state Day over Day */
    val dod = sqlContext.sql(
      """
        |select dbs.last_updated,
        |       dbs.state,
        |       infected as infected,
        |       case when dbs.state != 'New York' then cast(deaths/2 as int) else deaths end as deaths,
        |       recovered recovered,
        |       hospitalized hospitalized,
        |       discharged discharged
        |from (select last_updated,
        |             state,
        |             cast(sum(deaths) as Integer) deaths
        |      from covid
        |      where country = 'US'
        |      group by 1, 2 ) dbs
        |         join (
        |    select last_updated,
        |           state,
        |           cast(sum(cases) as Integer) infected,
        |           cast(sum(recovered) as Integer)   recovered,
        |           cast(sum(hospitalized) as Integer) hospitalized,
        |           cast(sum(discharged) as Integer)   discharged
        |    from covid
        |    where country = 'US'
        |      and state is not null
        |      and level = 'county'
        |    group by 1, 2) ibs on dbs.state = ibs.state and ibs.last_updated = dbs.last_updated
        |""".stripMargin)
      .withColumn("dod_infected",
        $"infected" -
          (lead($"infected", 1, 1) over Window.partitionBy($"state").orderBy($"last_updated".desc, $"infected".desc, $"deaths".desc)))
      .withColumn("dod_deaths",
        $"deaths" -
          (lead($"deaths", 1, 1) over Window.partitionBy($"state").orderBy($"last_updated".desc, $"infected".desc, $"deaths".desc)))
      .persist(StorageLevel.MEMORY_ONLY_SER_2)

    dod
      .select($"state")
      .distinct()
      .collect().map(_.toSeq
      .foreach(x => {
        dod
          .select($"last_updated", $"state",
            format_number($"infected", 0).as("infected"),
            format_number($"dod_infected", 0).as("dod_infected"),
            format_number($"deaths", 0).as("deaths"),
            format_number($"dod_deaths", 0).as("dod_deaths"))
          .filter($"state" === x.toString())
          .orderBy($"last_updated".desc)
          .take(25)
          .foreach(v => {
            stringBuilder.append(v.mkString("\t")).append(delim)
          })
        // dod.checkpoint()
        stringBuilder.append(delim)
      }
      ))


    sw.stop()
    stringBuilder.append("INFO QA process runtime (seconds): " + sw.getTime(TimeUnit.SECONDS) + "\n")
  }

}
