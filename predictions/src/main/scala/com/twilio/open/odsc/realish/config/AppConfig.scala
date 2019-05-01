package com.twilio.open.odsc.realish.config

import java.io.File

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object AppConfig {
  private val mapper = new ObjectMapper(new YAMLFactory)
  mapper.registerModule(DefaultScalaModule)

  def parse(configPath: String): AppConfig = {
    mapper.readValue(new File(configPath), classOf[AppConfig])
  }

}

@SerialVersionUID(100L)
case class AppConfig(
  sparkAppConfig: SparkAppConfig,
  streamingQueryConfig: StreamingQueryConfig
) extends Serializable

@SerialVersionUID(100L)
case class SparkAppConfig(
  appName: String,
  core: Map[String, String]
) extends Serializable

trait KafkaConsumerConfig {
  val topic: String
  val subscriptionType: String
  val conf: Map[String, String]
}

@SerialVersionUID(100L)
case class ConsumerConfig(
  topic: String,
  subscriptionType: String,
  conf: Map[String, String]
) extends KafkaConsumerConfig with Serializable

@SerialVersionUID(100L)
case class StreamingQueryConfig(
  streamName: String,
  triggerInterval: String,
  triggerEnabled: Boolean,
  windowInterval: String,
  watermarkInterval: String/*,
  options: Map[String, String]*/
) extends Serializable
