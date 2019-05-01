package com.twilio.open.odsc.realish.listeners

import kamon.Kamon
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object InsightsQueryListener {
  val log: Logger = LoggerFactory.getLogger(classOf[InsightsQueryListener])

  def apply(spark: SparkSession, restart: () => Unit): InsightsQueryListener = {
    new InsightsQueryListener(spark, restart)
  }

}

class InsightsQueryListener(sparkSession: SparkSession, restart: () => Unit) extends StreamingQueryListener {
  import InsightsQueryListener._
  private val streams = sparkSession.streams
  private val defaultTag = Map("app_name" -> sparkSession.sparkContext.appName)

  def doubleToLong(value: Double): Long = {
    value match {
      case a if a.isInfinite => 0L
      case b if b == Math.floor(b) => b.toLong
      case c => Math.rint(c).toLong
    }
  }

  override def onQueryStarted(event: QueryStartedEvent): Unit = {
    if (log.isDebugEnabled) log.debug(s"onQueryStarted queryName=${event.name} id=${event.id} runId=${event.runId}")
  }

  //https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/streaming/progress.scala
  override def onQueryProgress(progressEvent: QueryProgressEvent): Unit = {
    val progress = progressEvent.progress
    val inputRowsPerSecond = progress.inputRowsPerSecond
    val processedRowsPerSecond = progress.processedRowsPerSecond

    // note: leaving this here to remind that we can do fancy things with this for metrics sake
    /*progress.stateOperators.map { operator =>
      val numRowsTotal = operator.numRowsTotal
      val numRowsUpdated = operator.numRowsUpdated
      (
          "num.rows.total" -> numRowsTotal,
          "num.rows.updated" -> numRowsUpdated
      )
    }*/

    val sources = progress.sources.map { source =>
      val description = source.description
      val startOffset = source.startOffset
      val endOffset = source.endOffset
      val inputRows = source.numInputRows

      s"topic=$description startOffset=$startOffset endOffset=$endOffset numRows=$inputRows"
    }
    val tags = defaultTag + ( "stream_name" -> progress.name )
    Kamon.metrics.histogram("spark.query.progress.processed.rows.rate", tags).record(doubleToLong(processedRowsPerSecond))
    Kamon.metrics.histogram("spark.query.progress.input.rows.rate", tags).record(doubleToLong(inputRowsPerSecond))

    // todo - could take num.rows.total, given total percentage of records that will be watermarked going forwards... (simple metric that say loss_percentage due to watermark)

    // should give min, avg, max, watermark
    val eventTime = progress.eventTime
    if (eventTime != null) {

      log.info(s"event.time=${eventTime.asScala.mkString(",")}")
    }

    log.info(s"query.progress query=${progress.name} kafka=${sources.mkString(",")} inputRows/s=$inputRowsPerSecond processedRows/s=$processedRowsPerSecond durationMs=${progress.durationMs} sink=${progress.sink.json}")
  }

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
    log.warn(s"queryTerminated: $event")
    val possibleStreamingQuery = streams.get(event.id)
    if (possibleStreamingQuery != null) {
      val progress = possibleStreamingQuery.lastProgress
      val sources = progress.sources
      log.warn(s"last.progress.sources sources=$sources")
    }

    event.exception match {
      case Some(exception) =>
        log.warn(s"queryEndedWithException exception=$exception resetting.all.streams")
        restart()
      case None =>
    }
  }
}
