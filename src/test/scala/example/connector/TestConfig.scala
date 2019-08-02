package example.connector

import scala.collection.JavaConverters._

object TestConfig {

  val srcMap: Map[String, String] = Map(
    GitHubConnectorConfig.topicProperty -> "issues",
    GitHubConnectorConfig.ownerProperty -> "apache",
    GitHubConnectorConfig.repoProperty -> "kafka",
    GitHubConnectorConfig.batchSizeProperty -> "10",
    GitHubConnectorConfig.sinceTimestampProperty -> "2017-04-26T01:23:45Z"
  )

  val props: java.util.Map[String, String] = srcMap.asJava

  lazy val config = new GitHubConnectorConfig(props)

}
