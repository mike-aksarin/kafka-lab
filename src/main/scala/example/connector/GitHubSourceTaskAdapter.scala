package example.connector

import java.time.Instant

import example.connector.GitHubClient.RateLimit
import example.connector.GitHubSourceTask.{IssueOffset, PollResult}
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}
import scala.collection.JavaConverters._

class GitHubSourceTaskAdapter extends SourceTask {

  var taskOpt: Option[GitHubSourceTask] = None
  var offsetOpt: Option[IssueOffset] = None
  var rateLimit = RateLimit(9999, 9999, Instant.MAX)

  override def start(props: java.util.Map[String, String]): Unit = {
    val config = new GitHubConnectorConfig(props)
    val task = new GitHubSourceTask(config)
    taskOpt = Some(task)
    offsetOpt = Some(task.loadOffset(context))
  }

  override def poll(): java.util.List[SourceRecord] = {
    val task = taskOpt.getOrElse(throw new Error("Task should be initialized via `start` method"))
    val offset = offsetOpt.getOrElse(throw new Error("Offset should be initialized via `start` method"))
    val PollResult(records, nextOffset, nextRateLimit) =  task.poll(offset, rateLimit)
    offsetOpt = Some(nextOffset)
    rateLimit = nextRateLimit
    records.asJava
  }

  // Nothing required to stop the task.
  override def stop(): Unit = ()

  override def version(): String = VersionUtil.version

}
