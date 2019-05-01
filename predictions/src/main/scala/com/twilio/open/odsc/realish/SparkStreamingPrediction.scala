package com.twilio.open.odsc.realish

import java.sql.Timestamp

import com.twilio.open.odsc.realish.config.AppConfig
import com.twilio.open.odsc.realish.listeners.SparkApplicationListener
import com.twilio.open.odsc.realish.utils.RestartableStreamingApp
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.streaming.{DataStreamReader, OutputMode, Trigger}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

object SparkStreamingPrediction extends App {

  val logger: Logger = LoggerFactory.getLogger(classOf[SparkStreamingPrediction])
  logger.info(s"$args")

  val configPath = if (args.length > 0) args(0) else ""
  val config = AppConfig.parse(configPath)

  val sparkConf = new SparkConf()
    .setAppName(config.sparkAppConfig.appName)
    .setJars(SparkContext.jarOfClass(classOf[SparkStreamingPrediction]).toList)
    .setAll(config.sparkAppConfig.core)

  logger.info(s"sparkConfig: ${sparkConf.toDebugString} appConfig: $config")

  val session = SparkSession.builder
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  session.sparkContext.addSparkListener(SparkApplicationListener(session))

  //ChroniclerRecovery(config, session).monitoredRun()
}

@SerialVersionUID(1L)
case class Metric(
  timestamp: Timestamp,
  uuid: String,
  metric: String,
  value: Double,
  countryCode: String,
  callDirection: String
) extends Serializable

@SerialVersionUID(1L)
class SparkStreamingPrediction(config: AppConfig, override val spark: SparkSession)
  extends RestartableStreamingApp with Serializable {

  val inputDir: String = spark.sparkContext.getConf.get("spark.odsc.predictions.inputDir")
  val outputDir: String = spark.sparkContext.getConf.get("spark.odsc.predictions.outputDir")
  val checkpointPath: String = spark.sparkContext.getConf.get("spark.odsc.predictions.checkpointPath")
  val maxFilesPerTrigger: Int = spark.sparkContext.getConf.getInt("spark.odsc.predictions.maxFilesPerTrigger", 1)
  val metricEncoder: Encoder[Metric] = Encoders.product[Metric]

  override val logger: Logger = LoggerFactory.getLogger(classOf[SparkStreamingPrediction])

  lazy val fileRecoveryStream: DataStreamReader = fileStreamReader()

  def fileStreamReader(): DataStreamReader = spark.readStream
    .schema(metricEncoder.schema)
    .option("maxFilesPerTrigger", maxFilesPerTrigger)

  override def run(): Unit = {
    import spark.implicits._
    import org.apache.spark.sql.functions._

    assert(inputDir.nonEmpty, "spark.odsc.predictions.inputDir cannot be empty")
    assert(outputDir.nonEmpty, "spark.odsc.predictions.outputDir cannot be empty")
    assert(checkpointPath.nonEmpty, "spark.odsc.predictions.checkpointPath cannot be empty")

    val streamingQuery = fileRecoveryStream
      .parquet(inputDir)
      .withWatermark("timestamp", config.streamingQueryConfig.watermarkInterval)
      .groupBy(col("metric"), col("countryCode"), window($"timestamp", config.streamingQueryConfig.windowInterval))
      .agg(
        min("value") as "min",
        avg("value") as "mean",
        max("value") as "max",
        count("*") as "total"
      )
      .writeStream
      .format("parquet")
      .outputMode(OutputMode.Append())
      .queryName(config.streamingQueryConfig.streamName)
      .option("checkpointLocation", checkpointPath)
      .option("path", outputDir)
      .partitionBy("window","metric")

    if (config.streamingQueryConfig.triggerEnabled) {
      val trigger = Trigger.ProcessingTime(config.streamingQueryConfig.triggerInterval)
      logger.info(s"trigger.interval=${config.streamingQueryConfig.triggerInterval}")
      streamingQuery
        .trigger(trigger)
        .start()
    } else streamingQuery.start()

  }

}
