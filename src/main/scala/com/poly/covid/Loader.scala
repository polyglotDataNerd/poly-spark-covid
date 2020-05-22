package com.poly.covid

/*
aws s3 cp ~/sg-spark-ccpa/ s3://polyglotDataNerd-bigdata-utility/spark/sg-spark-ccpa --recursive --sse  --include "*" --exclude "*.DS_Store*" --exclude "*.iml*" --exclude "*dependency-reduced-pom.xml"
*/

import com.poly.utils._
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
    val stringBuilder: java.lang.StringBuffer = new java.lang.StringBuffer
    val utils: Utils = new Utils()
    /*local mac
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
      .config("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com")
      .config("spark.hadoop.fs.s3a.access.key", utils.getSSMParam("/s3/polyglotDataNerd/admin/AccessKey"))
      .config("spark.hadoop.fs.s3a.secret.key", utils.getSSMParam("/s3/polyglotDataNerd/admin/SecretKey"))
      .getOrCreate()
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext*/

    /**/ val sparkSession = SparkSession
      .builder()
      .appName("spark-COVIDLoader" + "-" + java.util.UUID.randomUUID())
      /* EMR 6.0.0 */
      .config("yarn.node-labels.enabled", "true")
      .config("yarn.node-labels.am.default-node-label-expression", "CORE")
      /* EMR 6.0.0 */
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", 2047)
      .config("org.apache.spark.shuffle.sort.SortShuffleManager", "tungsten-sort")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .config("spark.speculation", "false")
      .config("spark.sql.broadcastTimeout", "1600")
      .config("spark.network.timeout", "1600")
      .config("spark.debug.maxToStringFields", 1000)
      .config("spark.sql.orc.impl", "native")
      .config("spark.sql.orc.enableVectorizedReader", "true")
      .config("spark.sql.caseSensitive", "true")
      .config("spark.port.maxRetries", 256)
      .config("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com")
      .config("spark.hadoop.fs.s3a.multiobjectdelete.enable","false")
      .config("spark.hadoop.fs.s3a.fast.upload","true")
      .config("spark.hadoop.fs.s3a.access.key", utils.getSSMParam("/s3/polyglotDataNerd/admin/AccessKey"))
      .config("spark.hadoop.fs.s3a.secret.key", utils.getSSMParam("/s3/polyglotDataNerd/admin/SecretKey"))
      .enableHiveSupport()
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    new Analysis().run(sparkContext, sqlContext, stringBuilder)
    new Utils(config.getPropValues("emails"), config.getPropValues("fromemails"),
      "ETL Notification " + " SPARK: COVID-19 Loader",
      stringBuilder.toString()).sendEMail()

    sparkSession.stop()
    System.exit(0);
  }
}
