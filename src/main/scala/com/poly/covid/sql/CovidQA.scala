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
        |select last_updated,
        |       state,
        |       format_number(sum(cases), 0)     infected,
        |       format_number(sum(deaths), 0)    deaths,
        |       format_number(sum(recovered), 0) recovered,
        |       format_number(sum(hospitalized), 0) hospitalized,
        |       format_number(sum(discharged), 0) discharged
        |from (
        |         select  last_updated,
        |                         state,
        |                         county,
        |                         cases,
        |                         nvl(deaths,us_deaths_county) as deaths,
        |                         recovered,
        |                         hospitalized,
        |                         discharged
        |         from covid
        |         where country = 'United States'
        |           and state is not null
        |           and level = 'county') dset
        |group by 1,2
        |order by sum(cases) desc, state desc, last_updated desc
        |""".stripMargin)
      .persist(StorageLevel.MEMORY_ONLY)
      .filter($"last_updated" === date)
      .take(500)
      .foreach(v => {
        stringBuilder.append(v.mkString("\t")).append(delim)
      })
    stringBuilder.append(delim)
    sw.stop()
    stringBuilder.append("INFO QA process runtime (seconds): " + sw.getTime(TimeUnit.SECONDS) + "\n")
  }

}
