package com.poly.covid.sql

import com.poly.utils.Utils
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{format_number, lead}
import org.apache.spark.sql.types.{DateType, IntegerType}
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
        |        where Country_Region = 'US'
        |group by 1,2
        |order by 1 desc
        |""".stripMargin)
      .persist(StorageLevel.MEMORY_ONLY)
      .show(5, false)

    /* all state by day */
    sparkSession.sql(
      """
        |select dbs.last_updated,
        |       dbs.state,
        |       infected,
        |       deaths,
        |       format_number(recovered, 0) as recovered,
        |       format_number(hospitalized, 0) as hospitalized,
        |       format_number(discharged, 0) as  discharged
        |from (select last_updated,
        |             state,
        |             cast(sum(us_deaths_county) as Integer) deaths
        |      from covid
        |      where country = 'US'
        |      group by 1, 2 ) dbs
        |         join (
        |    select last_updated,
        |           state,
        |           cast(sum(us_confirmed_county) as Integer) infected,
        |           cast(sum(recovered) as Integer)   recovered,
        |           cast(sum(us_active_county) as Integer) hospitalized,
        |           cast(sum(discharged) as Integer)   discharged
        |    from covid
        |    where country = 'US'
        |      and state is not null
        |      --and level = 'county'
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
        |       infected,
        |       deaths,
        |       format_number(recovered, 0) as recovered,
        |       format_number(hospitalized, 0) as hospitalized,
        |       format_number(discharged, 0) as  discharged
        |from (select last_updated,
        |             state,
        |             cast(sum(us_deaths_county) as Integer) deaths
        |      from covid
        |      where country = 'US'
        |      group by 1, 2 ) dbs
        |         join (
        |    select last_updated,
        |           state,
        |           cast(sum(us_confirmed_county) as Integer) infected,
        |           cast(sum(recovered) as Integer)   recovered,
        |           cast(sum(us_active_county) as Integer) hospitalized,
        |           cast(sum(discharged) as Integer)   discharged
        |    from covid
        |    where country = 'US'
        |      and state is not null
        |      --and level = 'county'
        |    group by 1, 2) ibs on dbs.state = ibs.state and ibs.last_updated = dbs.last_updated
        |    order by infected desc, deaths desc
        |""".stripMargin)
      .withColumn("dod_infected",
        $"infected" -
          (lead($"infected", 1, 1) over Window.partitionBy($"state").orderBy($"last_updated".desc, $"infected".desc, $"deaths".desc)))
      .withColumn("dod_deaths",
        $"deaths" -
          (lead($"deaths", 1, 1) over Window.partitionBy($"state").orderBy($"last_updated".desc, $"infected".desc, $"deaths".desc)))
      .persist(StorageLevel.MEMORY_ONLY_SER_2)

    /* State Day over Day delta's Deaths and Affected */
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

    /* weekly snapshot of top 10 hard hit states */
    dod
      .select($"last_updated", $"state", $"infected", $"dod_infected".as("current_infected"))
      .withColumn("previous_day",
        (lead($"current_infected", 1, 1) over Window.partitionBy($"state").orderBy($"last_updated".desc, $"current_infected".desc)))
      .withColumn("minus3",
        (lead($"current_infected", 2, 1) over Window.partitionBy($"state").orderBy($"last_updated".desc, $"current_infected".desc)))
      .withColumn("minus4",
        (lead($"current_infected", 3, 1) over Window.partitionBy($"state").orderBy($"last_updated".desc, $"current_infected".desc)))
      .withColumn("minus5",
        (lead($"current_infected", 4, 1) over Window.partitionBy($"state").orderBy($"last_updated".desc, $"current_infected".desc)))
      .withColumn("minus6",
        (lead($"current_infected", 5, 1) over Window.partitionBy($"state").orderBy($"last_updated".desc, $"current_infected".desc)))
      .withColumn("previous_day_diff",
        $"current_infected" -
          (lead($"current_infected", 1, 1) over Window.partitionBy($"state").orderBy($"last_updated".desc, $"current_infected".desc)))
      .withColumn("minus3_diff",
        $"previous_day" -
          (lead($"previous_day", 1, 1) over Window.partitionBy($"state").orderBy($"last_updated".desc, $"previous_day".desc)))
      .withColumn("minus4_diff",
        $"minus3" -
          (lead($"minus3", 1, 1) over Window.partitionBy($"state").orderBy($"last_updated".desc, $"minus3".desc)))
      .withColumn("minus5_diff",
        $"minus4" -
          (lead($"minus4", 1, 1) over Window.partitionBy($"state").orderBy($"last_updated".desc, $"minus4".desc)))
      .withColumn("minus6_diff",
        $"minus5" -
          (lead($"minus5", 1, 1) over Window.partitionBy($"state").orderBy($"last_updated".desc, $"minus5".desc)))
      .orderBy($"last_updated".cast(DateType).desc, $"dod_infected".cast(IntegerType).desc)
      .select(
        $"last_updated",
        $"state",
        format_number($"infected", 0).as("infected"),
        format_number($"current_infected", 0).as("current_infected"),
        format_number($"previous_day", 0).as("previous_day"),
        format_number($"minus3", 0).as("minus3"),
        format_number($"minus4", 0).as("minus4"),
        format_number($"minus5", 0).as("minus5"),
        format_number($"minus6", 0).as("minus6"),
        format_number($"previous_day_diff", 0).as("previous_day_diff"),
        format_number($"minus3_diff", 0).as("minus3_diff"),
        format_number($"minus4_diff", 0).as("minus4_diff"),
        format_number($"minus5_diff", 0).as("minus5_diff"),
        format_number($"minus6_diff", 0).as("minus6_diff")
      )
      .limit(10)
      .show()
  }


}
