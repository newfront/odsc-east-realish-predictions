package com.twilio.open.odsc.realish

import java.sql.Timestamp
import java.time.Instant
import java.util.{Random, UUID}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoders, SQLContext, SparkSession}
import org.scalatest.{FunSuite, Matchers}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import scala.concurrent.duration._

class StreamingPredictionsSpec extends FunSuite with Matchers with SharedSparkSql {

  override def conf: SparkConf = {
    new SparkConf()
      .setMaster("local[*]")
      .setAppName("odsc-spark-utils")
      .set("spark.ui.enabled", "false")
      .set("spark.app.id", appID)
      .set("spark.driver.host", "localhost")
      .set("spark.sql.session.timeZone", "UTC")
  }

  final val notRandomRandom = {
    val generator = new Random
    generator.setSeed(100L)
    generator
  }

  test("should stream in some mock data for fun") {
    implicit val spark: SparkSession = sparkSql
    import spark.implicits._
    implicit val sqlContext: SQLContext = spark.sqlContext

    implicit val metricEncoder = Encoders.product[Metric]
    val metricData = MemoryStream[Metric]

    val startingInstant = Instant.now()

    val backingData = (1 to 10000).map(offset => {
      val metric = if (offset % 2 == 0) "loss_percentage" else "connect_duration"
      val nextLoss = notRandomRandom.nextDouble() * notRandomRandom.nextInt(100)
      Metric(
        Timestamp.from(startingInstant.minusSeconds(offset)),
        UUID.randomUUID().toString,
        metric,
        value = if (metric == "loss_percentage") nextLoss else notRandomRandom.nextDouble() * notRandomRandom.nextInt(240),
        countryCode = if (offset % 8 == 0) "US" else "BR",
        callDirection = if (metric == "loss_percentage") "inbound" else "outbound"
      )
    })
    val processingTimeTrigger = Trigger.ProcessingTime(2.seconds)


    val streamingQuery = metricData.toDF()
      .withWatermark("timestamp", "2 hours")
      .groupBy(col("metric"), col("countryCode"), window($"timestamp", "5 minutes"))
      .agg(
        min("value") as "min",
        avg("value") as "mean",
        max("value") as "max",
        count("*") as "total"
      )
      .writeStream
      .format("memory")
      .queryName("datastream")
      .outputMode(OutputMode.Append())
      .trigger(processingTimeTrigger)
      .start()

    metricData.addData(backingData)

    streamingQuery.processAllAvailable()

    spark.sql("select * from datastream").show(20, false)

    val checkChange = spark.sql("select * from datastream")
      .groupBy("metric","countryCode")
      .agg(
        sum("total") as "total",
        avg("mean") as "mean"
      )

    checkChange.show(20, false)

    // now can do interesting things with minor back tracking...

    streamingQuery.stop()

  }

}
