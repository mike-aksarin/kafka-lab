package example.connector

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector

/** Custom Kafka connector that gets a stream
  * of issues and pull requests for your chosen GitHub repository.
  * This is an example from Stephane Maarek
  * "Apache Kafka Series - Kafka Connect Hands-on Learning" course.
  *
  * @see Udemy course "Apache Kafka Series - Kafka Connect Hands-on Learning" —
  *      [[https://www.udemy.com/kafka-connect/]]
  * @see Original java sources by Stephane Maarek —
  *      [[https://github.com/simplesteph/kafka-connect-github-source]]
  */
class GitHubSourceConnector extends SourceConnector {

  private var propsOpt: Option[java.util.Map[String, String]] = None

  override def start(props: java.util.Map[String, String]): Unit = {
    propsOpt = Some(props)
  }

  // Do things that are necessary to stop your connector.
  // nothing is necessary to stop for this connector
  override def stop(): Unit = ()

  override def taskConfigs(maxTasks: Int): java.util.List[java.util.Map[String, String]] = {
    val props = propsOpt.getOrElse(throw new Error("No configuration! Should be set via `start` method"))
    java.util.Collections.singletonList(props)
  }

  override def taskClass(): Class[_ <: Task] = classOf[GitHubSourceTaskAdapter]

  override def config(): ConfigDef = GitHubConnectorConfig.configDef

  override def version(): String = VersionUtil.version
}
