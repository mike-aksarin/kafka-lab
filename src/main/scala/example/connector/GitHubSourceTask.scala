package example.connector

import java.time.Instant

import example.connector.GitHubClient.{Issue, PullRequest, RateLimit, User}
import example.connector.GitHubSourceTask._
import example.connector.JavaMapUtil._

import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct, Timestamp}
import org.apache.kafka.connect.source.{SourceRecord, SourceTaskContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class GitHubSourceTask(config: GitHubConnectorConfig) {

  private val log = LoggerFactory.getLogger(classOf[GitHubSourceTask])
  val client: GitHubClient = new GitHubClient(config)

  def loadOffset(context: SourceTaskContext): IssueOffset = {
    val offsetReader = context.offsetStorageReader()
    val maybeOffsetMap = Option(offsetReader.offset(sourcePartition.asJava))
    parseOffset(maybeOffsetMap, defaultOffset(config))
  }

  def poll(offset: IssueOffset, rateLimit: RateLimit): PollResult = {
    client.sleepIfNeeded(rateLimit)
    val response = client.getIssues(offset.nextPageToVisit, offset.updatedAt)
    if (response.nonEmpty) {
      log.info(s"Fetched ${response.issuesSize} records")
    }
    val nextRateLimit = response.rateLimit
    val nextOffset = if (response.issuesSize == 100) {
      val nextPage = offset.nextPageToVisit + 1
      offset.copy(nextPageToVisit = nextPage)
    } else {
      val nextQuerySince = offset.updatedAt.plusSeconds(1)
      offset.copy(updatedAt = nextQuerySince, nextPageToVisit = 1)
    }
    val records = response.mapIssues { issue => buildSourceRecord(issue, nextOffset) }
    if (nextOffset != offset) client.sleep(nextRateLimit)
    PollResult(records, nextOffset, nextRateLimit)
  }

  def buildSourceRecord(issue: Issue, nextOffset: IssueOffset): SourceRecord = {
    new SourceRecord(
      sourcePartition.asJava,
      sourceOffset(nextOffset, issue.updatedAt).asJava,
      config.topic,
      null, // Kafka partition will be inferred by the framework
      keySchema,
      recordKey(issue),
      issueSchema,
      recordValue(issue),
      issue.updatedAt.toEpochMilli
    )
  }

  def recordKey(issue: Issue): Struct = {
    new Struct(keySchema)
      .put(IssuePartition.ownerField, config.owner)
      .put(IssuePartition.repoField, config.repo)
      .put(Issue.numberField, issue.number)
  }

  def recordValue(issue: Issue): Struct = {
    val issueBasic = issueBasicStruct(issue)
    val pullRequestOpt = issue.pullRequest.map(pullRequestStruct)
   putOptional(issueBasic, Issue.pullRequestField, pullRequestOpt)
  }

  def putOptional(basic: Struct, key: String, option: Option[Struct]): Struct = {
    option.fold(basic)(basic.put(key, _))
  }

  def issueBasicStruct(issue: Issue): Struct = {
    new Struct(issueSchema)
      .put(Issue.urlField, issue.url)
      .put(Issue.htmlUrlField, issue.htmlUrl)
      .put(Issue.titleField, issue.title)
      .put(Issue.createdAtField, java.util.Date.from(issue.createdAt))
      .put(Issue.updatedAtField, java.util.Date.from(issue.updatedAt))
      .put(Issue.numberField, issue.number)
      .put(Issue.stateField, issue.state)
      .put(Issue.userField, userStruct(issue.user))
  }

  def userStruct(user: User): Struct =
    new Struct(userSchema)
      .put(User.urlField, user.url)
      .put(User.htmlUrlField, user.htmlUrl)
      .put(User.idField, user.id)
      .put(User.loginField, user.login)

  def pullRequestStruct(pullRequest: PullRequest): Struct = {
    new Struct(pullRequestSchema)
      .put(PullRequest.urlField, pullRequest.url)
      .put(PullRequest.htmlUrlField, pullRequest.htmlUrl)
  }

  val sourcePartition: Map[String, String] = Map(
    IssuePartition.ownerField -> config.owner,
    IssuePartition.repoField -> config.repo
  )

  def sourceOffset(nextOffset: IssueOffset, updatedAt: Instant): Map[String, String] = Map(
    IssueOffset.updatedAtField -> maxInstant(updatedAt, nextOffset.updatedAt).toString,
    IssueOffset.nextPageField ->  nextOffset.nextPageToVisit.toString
  )

  def maxInstant(i1: Instant, i2: Instant): Instant = {
    if (i1.compareTo(i2) > 0) i1 else i2
  }

}

object GitHubSourceTask {

  val issueSchemaKeyName = "example.connector.github.IssueKey"
  val issueSchemaValueName = "example.connector.github.IssueValue"
  val userSchemaName = "example.connector.github.User"
  val pullRequestSchemaName = "example.connector.github.PullRequest"

  case class PollResult(
    records: Seq[SourceRecord],
    nextOffset: IssueOffset,
    rateLimit: RateLimit
  )

  object IssuePartition {
    def ownerField = "owner"
    def repoField = "repo"
  }

  case class IssueOffset(
    updatedAt: Instant,
    nextPageToVisit: Int
  )

  object IssueOffset {
    def updatedAtField = "updated_at"
    def nextPageField = "next_page"
  }

  def defaultOffset(config: GitHubConnectorConfig): IssueOffset = {
    IssueOffset(config.sinceTimestamp, 1)
  }

  def parseOffset(javaMapOpt: Option[java.util.Map[String, AnyRef]],
                  defaultOffset:  => IssueOffset): IssueOffset = {
    javaMapOpt.fold(defaultOffset) { offsetMap =>
      IssueOffset(
        offsetMap.getInstant(IssueOffset.updatedAtField, defaultOffset.updatedAt),
        offsetMap.getInt(IssueOffset.nextPageField, defaultOffset.nextPageToVisit)
      )
    }
  }

  val keySchema: Schema = SchemaBuilder.struct()
    .name(issueSchemaKeyName)
    .version(1)
    .field(IssuePartition.ownerField, Schema.STRING_SCHEMA)
    .field(IssuePartition.repoField, Schema.STRING_SCHEMA)
    .field(Issue.numberField, Schema.INT32_SCHEMA)
    .build()

  val userSchema: Schema = SchemaBuilder.struct()
    .name(userSchemaName)
    .version(1)
    .field(User.urlField, Schema.STRING_SCHEMA)
    .field(User.htmlUrlField, Schema.STRING_SCHEMA)
    .field(User.idField, Schema.INT32_SCHEMA)
    .field(User.loginField, Schema.STRING_SCHEMA)
    .build()

  // optional schema
  val pullRequestSchema: Schema = SchemaBuilder.struct()
    .name(pullRequestSchemaName)
    .version(1)
    .field(PullRequest.urlField, Schema.STRING_SCHEMA)
    .field(PullRequest.htmlUrlField, Schema.STRING_SCHEMA)
    .optional()
    .build()

  val issueSchema: Schema = SchemaBuilder.struct()
    .name(issueSchemaValueName)
    .version(1)
    .field(Issue.urlField, Schema.STRING_SCHEMA)
    .field(Issue.htmlUrlField, Schema.STRING_SCHEMA)
    .field(Issue.titleField, Schema.STRING_SCHEMA)
    .field(Issue.createdAtField, Timestamp.SCHEMA)
    .field(Issue.updatedAtField, Timestamp.SCHEMA)
    .field(Issue.numberField, Schema.INT32_SCHEMA)
    .field(Issue.stateField, Schema.STRING_SCHEMA)
    .field(Issue.userField, userSchema)
    .field(Issue.pullRequestField, pullRequestSchema)
    .build()

}
