package example.connector

import java.time.{Instant, ZonedDateTime}

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

import GitHubConnectorConfig._

class GitHubConnectorConfig(properties: java.util.Map[String, String])
  extends AbstractConfig(GitHubConnectorConfig.configDef, properties) {

  def topic: String = getString(topicProperty)
  def owner: String = getString(ownerProperty)
  def repo: String = getString(repoProperty)
  def sinceTimestamp: Instant = Instant.parse(getString(sinceTimestampProperty))
  def batchSize: Int = getInt(batchSizeProperty)
  def authUser: String = getString(authUserProperty)
  def authPassword: String = getString(authPasswordProperty)
}

object GitHubConnectorConfig {

  /** Kafka topic to write to */
  val topicProperty = "topic"

  /** Owner of a GitHub repository you'd like to follow */
  val ownerProperty = "github.owner"

  /** GitHub repository you'd like to follow */
  val repoProperty = "github.repo"

  /** Only issues updated at or after this time are returned.
    * This is a timestamp in ISO 8601 format: YYYY-MM-DDTHH:MM:SSZ.
    * Defaults to a year from first launch.
    */
  val sinceTimestampProperty = "since.timestamp"

  /** Number of data points to retrieve at a time. Defaults to 100 (max value) */
  val batchSizeProperty = "batch.size"

  /** Optional Username to authenticate calls */
  val authUserProperty = "auth.username"

  /** Optional Password to authenticate calls */
  val authPasswordProperty = "auth.password"

  val configDef: ConfigDef = {
    new ConfigDef()
      .define(topicProperty, Type.STRING, Importance.HIGH,
        "Kafka topic to write to")

      .define(ownerProperty, Type.STRING, Importance.HIGH,
        "Owner of a GitHub repository you'd like to follow")

      .define(repoProperty, Type.STRING, Importance.HIGH,
        "GitHub repository you'd like to follow")

      .define(batchSizeProperty, Type.INT, defaultBatchSize,
        new BatchSizeValidator(defaultBatchSize), Importance.LOW,
        "Number of data points to retrieve at a time. Defaults to 100 (max value)")

      .define(sinceTimestampProperty, Type.STRING, defaultSinceTimestamp.toString,
        new TimestampValidator, Importance.HIGH,
        """Only issues updated at or after this time are returned.
          |This is a timestamp in ISO 8601 format: YYYY-MM-DDTHH:MM:SSZ.
          |Defaults to a year from first launch.""".stripMargin)

      .define(authUserProperty, Type.STRING, "", Importance.HIGH,
        "Optional Username to authenticate calls")

      .define(authPasswordProperty, Type.PASSWORD, "", Importance.HIGH,
        "Optional Password to authenticate calls")
  }

  def defaultBatchSize: Int = 100
  def defaultSinceTimestamp: Instant =  ZonedDateTime.now.minusYears(1).toInstant
}
