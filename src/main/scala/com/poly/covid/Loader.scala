package com.poly.covid

/*
aws s3 cp ~/sg-spark-ccpa/ s3://polyglotDataNerd-bigdata-utility/spark/sg-spark-ccpa --recursive --sse  --include "*" --exclude "*.DS_Store*" --exclude "*.iml*" --exclude "*dependency-reduced-pom.xml"
*/

import com.poly.Utils._
import com.poly.covid.sql.Analysis
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession


/**
 * Created by gbartolome on 11/10/17.
 */
object Loader extends java.io.Serializable {
  val config: ConfigProps = new ConfigProps()
  /*set logger*/
  System.setProperty("logfile.name", "/var/tmp/spark.log")
  config.loadLog4jprops()
  /* non verbose logging */
  Logger.getLogger(classOf[RackResolver]).getLevel
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    runSpark()
  }

  def runSpark(): Unit = {
    val stringBuilder: java.lang.StringBuilder = new java.lang.StringBuilder
    val utils: Utils = new Utils()
    /*local mac*/
    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkLocalMac")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.rpc.message.maxSize", "2047")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "2047")
      .config("org.apache.spark.shuffle.sort.SortShuffleManager", "tungsten-sort")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .config("spark.speculation", "false")
      .config("spark.hadoop.mapred.output.compress", "true")
      .config("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
      .config("spark.mapreduce.output.fileoutputformat.compress", "true")
      .config("spark.mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec")
      .config("spark.debug.maxToStringFields", "500")
      .config("spark.sql.caseSensitive", "false")
      .config("spark.hadoop.fs.s3a.access.key", utils.getSSMParam("/s3/polyglotDataNerd/admin/AccessKey"))
      .config("spark.hadoop.fs.s3a.secret.key", utils.getSSMParam("/s3/polyglotDataNerd/admin/SecretKey"))
      .getOrCreate()
    val sc = sparkSession.sparkContext
    val sql = sparkSession.sqlContext

    /* val sparkSession = SparkSession
      .builder()
      .appName("spark-ccpa-" + sourceName + "-" + java.util.UUID.randomUUID())
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", 2047)
      .config("org.apache.spark.shuffle.sort.SortShuffleManager", "tungsten-sort")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .config("spark.speculation", "false")
      /*needs to have when merging a lot of small files*/
      .config("spark.rpc.message.maxSize", 2047)
      .config("spark.debug.maxToStringFields", 10000)
      /*needs to have when merging a lot of small files*/
      .config("spark.driver.memory", "120G")
      .config("spark.executor.memory", "120G")
      .config("spark.executor.memoryOverhead", "110G")
      .config("spark.driver.memoryOverhead", "110G")
      //.config("spark.debug.maxToStringFields", 500)
      .config("spark.driver.maxResultSize", "120G")
      /*increase heap space https://stackoverflow.com/questions/21138751/spark-java-lang-outofmemoryerror-java-heap-space*/
      .config("spark.memory.offHeap.enabled", true)
      .config("spark.memory.offHeap.size", "120g")
      /*https://developer.ibm.com/hadoop/2016/07/18/troubleshooting-and-tuning-spark-for-heavy-workloads*/
      .config("spark.sql.broadcastTimeout", "1600")
      .config("spark.network.timeout", "1600")
      .config("spark.debug.maxToStringFields", 1000)
      .config("spark.sql.orc.impl", "native")
      .config("spark.sql.orc.enableVectorizedReader", "true")
      .config("spark.sql.caseSensitive", "true")
      .config("spark.port.maxRetries", 256)
      //.config("spark.sql.session.timeZone", "UTC")
      .enableHiveSupport()
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    val sc = sparkSession.sparkContext
    val sql = sparkSession.sqlContext*/

    new Analysis().run(sparkSession, sc, sql, stringBuilder)

    new Utils(config.getPropValues("emails"), config.getPropValues("fromemails"),
      "ETL Notification " + " SPARK: COVID-19 Loader",
      stringBuilder.toString()).sendEMail()

    sparkSession.stop()
    System.exit(0);
  }
}
