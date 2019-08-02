package example.connector

import java.time.Instant

import example.connector.GitHubClient.{Issue, IssuesResponse, RateLimit, parseIssue}
import kong.unirest._
import org.apache.kafka.connect.errors.ConnectException
import org.json.JSONObject
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.JavaConverters._

class GitHubClient(config: GitHubConnectorConfig) {

  private val log = LoggerFactory.getLogger(classOf[GitHubClient])

  @tailrec
  final def getIssues(page: Int, since: Instant): IssuesResponse = {
    val url = issuesUrl(page, since)
    val resp = sendRequest(url)
    resp.getStatus match {
      case 200 => IssuesResponse(parseResponse(resp.getBody), parseHeaders(resp.getHeaders))
      case 401 => throw new ConnectException("Bad GitHub credentials provided, please edit your config")
      case 403 => val rateLimit = parseHeaders(resp.getHeaders)
        log.info(resp.getBody.getObject.getString("message"))
        log.info(s"Your rate limit is $rateLimit")
        log.info(s"Sleeping for ${rateLimit.resetTimeMillis} millis")
        Thread.sleep(rateLimit.resetTimeMillis)
        getIssues(page, since)
      case status =>
        log.info(url)
        log.info(s"$status ${resp.getBody}")
        log.info("Unknown error: Sleeping 5 seconds before re-trying")
        Thread.sleep(5000)
        getIssues(page, since)
    }
  }

  def issuesUrl(page: Int, since: Instant): String =
    s"https://api.github.com/repos/${config.owner}/${config.repo}/issues?" +
      s"page=$page&per_page=${config.batchSize}&since=$since&" +
      "state=all&direction=asc&sort=updated"

  def sendRequest(url: String): HttpResponse[JsonNode] = {
    log.info(s"get $url")
    val req = Unirest.get(url)
    val reqWithAuth = if (config.authUser.isEmpty) req else {
      req.basicAuth(config.authUser, config.authPassword)
    }
    reqWithAuth.asJson()
  }

  def parseResponse(body: JsonNode): Seq[Issue] = {
    body.getArray.iterator().asScala.map { obj =>
      parseIssue(obj.asInstanceOf[JSONObject])
    }.toSeq
  }

  def parseHeaders(headers: Headers): RateLimit = RateLimit(
    callsLimit = headers.getFirst("X-RateLimit-Limit").toInt,
    remainingCalls = headers.getFirst("X-RateLimit-Remaining").toInt,
    resetAt = Instant.ofEpochSecond(headers.getFirst("X-RateLimit-Reset").toLong)
  )

  def sleep(rateLimit: RateLimit): Unit = {
    log.info(s"Sleeping for ${rateLimit.sleepPerCallMillis} millis")
    Thread.sleep(rateLimit.sleepPerCallMillis)
  }

  def sleepIfNeeded(rateLimit: RateLimit): Unit = {
    import rateLimit.remainingCalls
    if (remainingCalls > 0 && remainingCalls < 10) {
      log.info(s"Approaching limit soon, you have $remainingCalls requests left")
      sleep(rateLimit)
    }
  }

}

object GitHubClient {

  case class IssuesResponse(issues: Seq[Issue], rateLimit: RateLimit) {
    def nonEmpty: Boolean = issues.nonEmpty
    def issuesSize: Int = issues.size
    def mapIssues[T](f: Issue => T): Seq[T] = issues.map(f)
  }

  case class Issue(
    //id: Int,
    url: String,
    htmlUrl: String,
    title: String,
    //body: String,
    createdAt: Instant,
    updatedAt: Instant,
    number: Int,
    state: String,
    user: User,
    pullRequest: Option[PullRequest]
  )

  object Issue {
    def urlField: String = "url"
    def htmlUrlField: String = "html_url"
    def titleField: String = "title"
    def createdAtField: String = "created_at"
    def updatedAtField: String = "updated_at"
    def numberField: String = "number"
    def stateField: String = "state"
    def userField: String = "user"
    def pullRequestField: String = "pull_request"
  }

  def parseIssue(json: JSONObject): Issue = Issue(
    url = json.getString(Issue.urlField),
    htmlUrl = json.getString(Issue.htmlUrlField),
    title = json.getString(Issue.titleField),
    createdAt = Instant.parse(json.getString(Issue.createdAtField)),
    updatedAt = Instant.parse(json.getString(Issue.updatedAtField)),
    number = json.getInt(Issue.numberField),
    state = json.getString(Issue.stateField),
    user = parseUser(json.getJSONObject(Issue.userField)),
    pullRequest = parsePullRequestIfExists(json)
  )

  def parsePullRequestIfExists(issueJson: JSONObject): Option[PullRequest] = {
    if (issueJson.has(Issue.pullRequestField))
      Some(parsePullRequest(issueJson.getJSONObject(Issue.pullRequestField)))
    else
      None
  }

  case class User(
    id: Int,
    login: String,
    url: String,
    htmlUrl: String,
  )

  object User {
    def idField: String = "id"
    def loginField: String = "login"
    def urlField: String = "url"
    def htmlUrlField: String = "html_url"
  }

  def parseUser(json: JSONObject): User = User(
    id = json.getInt(User.idField),
    login = json.getString(User.loginField),
    url = json.getString(User.urlField),
    htmlUrl = json.getString(User.htmlUrlField),
  )

  case class PullRequest(
    url: String,
    htmlUrl: String
   )

  object PullRequest {
    def urlField: String = "url"
    def htmlUrlField: String = "html_url"
  }

  def parsePullRequest(json: JSONObject): PullRequest =  PullRequest(
    url = json.getString(PullRequest.urlField),
    htmlUrl = json.getString(PullRequest.htmlUrlField)
  )

  case class RateLimit(
    callsLimit: Int,
    remainingCalls: Int,
    resetAt: Instant
  ) {

    lazy val resetTimeMillis: Long =
      (resetAt.getEpochSecond - Instant.now().getEpochSecond) * 100

    lazy val sleepPerCallMillis: Long =
      Math.ceil(resetTimeMillis / remainingCalls).toLong

    override def toString: String = {
      s"Calls limit: $callsLimit, Remaining calls: $remainingCalls," +
      s"The limit will reset at $resetAt}"
    }
  }

}
