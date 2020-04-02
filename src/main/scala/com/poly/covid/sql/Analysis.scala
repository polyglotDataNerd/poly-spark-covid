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
        .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
      jhu.createOrReplaceTempView("jhu")
      jhu.show(10, false)

      val cds = sql
        .read
        .schema(schemas.cds())
        .option("header", "true")
        .csv("s3a://poly-testing/covid/cds/*")
        .distinct()
        .persist(StorageLevel.MEMORY_ONLY_SER_2)
      cds.createOrReplaceTempView("cds")
      cds.show(10, false)


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

