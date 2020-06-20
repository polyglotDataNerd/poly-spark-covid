package com.poly.covid.sql

import com.poly.utils.Utils
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.lead
import org.apache.spark.storage.StorageLevel

class Insights {

  private val utils: Utils = new Utils
  private val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
  private val date = format.format(DateUtils.addDays(new java.util.Date(), -1))


  def runInsights(): Unit = {
    /*local*/
    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkLocalMac")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.rpc.message.maxSize", 2047)
      .config("spark.sql.shuffle.partitions", "10")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", 2047)
      .config("org.apache.spark.shuffle.sort.SortShuffleManager", "tungsten-sort")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .config("spark.speculation", "false")
      .config("spark.hadoop.mapred.output.compress", "true")
      .config("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
      .config("spark.mapreduce.output.fileoutputformat.compress", "true")
      .config("spark.mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec")
      .config("spark.debug.maxToStringFields", 500)
      .config("spark.sql.caseSensitive", "false")
      .config("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com")
      .config("spark.hadoop.fs.s3a.access.key", utils.getSSMParam("/s3/polyglotDataNerd/admin/AccessKey"))
      .config("spark.hadoop.fs.s3a.secret.key", utils.getSSMParam("/s3/polyglotDataNerd/admin/SecretKey"))
      .getOrCreate()

    val sql = sparkSession.sqlContext
    import sql.implicits._

    /* matches graph https://coronavirus.jhu.edu/map.html */
    sparkSession.sql(
      """
        select last_updated, format_number(sum(Deaths), 0) as us_deaths, format_number(sum(Confirmed), 0) us_affected
        |from jhu
            where Country_Region = 'US'
        |group by 1
        |order by 1 desc
        |""".stripMargin)
      .persist(StorageLevel.MEMORY_ONLY)
      .show(5, false)

    /* all state by day */
    sparkSession.sql(
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
      .show(500, false)


    /* state by state Day over Day */
    val dod = sparkSession.sql(
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
          .select($"last_updated", $"state", $"infected", $"dod_infected", $"deaths", $"dod_deaths")
          .filter($"state" === x.toString())
          .orderBy($"last_updated".desc)
          .show(25, false)
      }
      ))
  }


}
