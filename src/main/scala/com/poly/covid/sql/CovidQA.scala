package com.poly.covid.sql

import java.util.concurrent.TimeUnit

import com.poly.covid.utility.Schemas
import org.apache.commons.lang3.time.{DateUtils, StopWatch}
import org.apache.spark.sql.{SQLContext}
import org.apache.spark.storage.StorageLevel

class CovidQA {

  private val sw = new StopWatch
  private val schemas: Schemas = new Schemas
  private val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
  private val date = format.format(DateUtils.addDays(new java.util.Date(), -1))
  private val delim = "\n"

  def runQA(sqlContext: SQLContext, stringBuilder: java.lang.StringBuffer): Unit = {
    sw.start()
    val df = sqlContext
      .read
      .option("header", true)
      .option("delimiter", "\t")
      .option("quote", "\"")
      .option("escape", "\"")
      .schema(schemas.covidStruct())
      .csv("s3a://poly-testing/covid/combined/*")
      .distinct()
      .persist(StorageLevel.MEMORY_ONLY_SER_2)
    df.createOrReplaceTempView("covid")
    println(df.collect().size)

    sqlContext
      .read
      .option("header", true)
      .schema(schemas.jhu())
      .csv("s3a://poly-testing/covid/jhu/transformed/*")
      .distinct()
      .persist(StorageLevel.MEMORY_ONLY_SER_2)
      .createOrReplaceTempView("jhu")

    sqlContext
      .read
      .option("header", true)
      .schema(schemas.cds())
      .csv("s3a://poly-testing/covid/cds/*")
      .distinct()
      .persist(StorageLevel.MEMORY_ONLY_SER_2)
      .createOrReplaceTempView("cds")

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
        select last_updated, format_number(sum(Deaths), 0) as us_deaths, format_number(sum(Confirmed), 0) us_affected
        |from jhu
        where Country_Region = 'US'
        |group by 1
        |order by 1 desc
        |""".stripMargin)
      .persist(StorageLevel.MEMORY_ONLY)
      .take(5)
      .foreach(v => {
        stringBuilder.append(v.mkString("\t")).append(delim)
      })
    stringBuilder.append(delim)

    stringBuilder.append(String.format("%s", "Combined")).append(delim)
    sqlContext.sql(
      """
        select last_updated, count(distinct last_updated||level||county||state||country) as combined_records
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
    sqlContext.sql(
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
        |      where country = 'United States'
        |      group by 1, 2 ) dbs
        |         join (
        |    select last_updated,
        |           state,
        |           cast(sum(cases) as Integer) infected,
        |           cast(sum(recovered) as Integer)   recovered,
        |           cast(sum(hospitalized) as Integer) hospitalized,
        |           cast(sum(discharged) as Integer)   discharged
        |    from covid
        |    where country = 'United States'
        |      and state is not null
        |      and level = 'county'
        |    group by 1, 2) ibs on dbs.state = ibs.state and ibs.last_updated = dbs.last_updated
        |    order by infected desc, deaths desc
        |""".stripMargin)
      .filter($"last_updated" === date)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
      .take(500)
      .foreach(v => {
        stringBuilder.append(v.mkString("\t")).append(delim)
      })
    stringBuilder.append(delim)
    sw.stop()
    stringBuilder.append("INFO QA process runtime (seconds): " + sw.getTime(TimeUnit.SECONDS) + "\n")
  }

}
