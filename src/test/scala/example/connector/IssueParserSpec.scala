package example.connector

import example.connector.GitHubClient.{Issue, PullRequest, User}
import org.json.JSONObject
import org.scalatest.{FlatSpec, Matchers}

class IssueParserSpec extends FlatSpec with Matchers {

  private val issueString = """{
      "url": "https://api.github.com/repos/apache/kafka/issues/2800",
      "repository_url": "https://api.github.com/repos/apache/kafka",
      "labels_url": "https://api.github.com/repos/apache/kafka/issues/2800/labels{/name}",
      "comments_url": "https://api.github.com/repos/apache/kafka/issues/2800/comments",
      "events_url": "https://api.github.com/repos/apache/kafka/issues/2800/events",
      "html_url": "https://github.com/apache/kafka/pull/2800",
      "id": 219155037,
      "number": 2800,
      "title": "added interface to allow producers to create a ProducerRecord without…",
      "user": {
        "login": "simplesteph",
        "id": 20851561,
        "avatar_url": "https://avatars3.githubusercontent.com/u/20851561?v=3",
        "gravatar_id": "",
        "url": "https://api.github.com/users/simplesteph",
        "html_url": "https://github.com/simplesteph",
        "followers_url": "https://api.github.com/users/simplesteph/followers",
        "following_url": "https://api.github.com/users/simplesteph/following{/other_user}",
        "gists_url": "https://api.github.com/users/simplesteph/gists{/gist_id}",
        "starred_url": "https://api.github.com/users/simplesteph/starred{/owner}{/repo}",
        "subscriptions_url": "https://api.github.com/users/simplesteph/subscriptions",
        "organizations_url": "https://api.github.com/users/simplesteph/orgs",
        "repos_url": "https://api.github.com/users/simplesteph/repos",
        "events_url": "https://api.github.com/users/simplesteph/events{/privacy}",
        "received_events_url": "https://api.github.com/users/simplesteph/received_events",
        "type": "User",
        "site_admin": false
      },
      "labels": [],
      "state": "closed",
      "locked": false,
      "assignee": null,
      "assignees": [],
      "milestone": null,
      "comments": 12,
      "created_at": "2017-04-04T06:47:09Z",
      "updated_at": "2017-04-19T22:36:21Z",
      "closed_at": "2017-04-19T22:36:21Z",
      "pull_request": {
        "url": "https://api.github.com/repos/apache/kafka/pulls/2800",
        "html_url": "https://github.com/apache/kafka/pull/2800",
        "diff_url": "https://github.com/apache/kafka/pull/2800.diff",
        "patch_url": "https://github.com/apache/kafka/pull/2800.patch"
      },
      "body": "… specifying a partition, making it more obvious that the parameter partition can be null"
    }"""

  private val issueJson = new JSONObject(issueString)

  "GitHubClient" should "parse Issue from JSON " in {
    val issue = GitHubClient.parseIssue(issueJson)
    validateIssue(issue)
  }

  "GitHubSourceTask" should "convert Issue to Struct" in {
    val task = new GitHubSourceTask(TestConfig.config)
    val issue = GitHubClient.parseIssue(issueJson)
    val struct = task.recordValue(issue)
    struct.get(Issue.numberField) shouldBe 2800
    struct.get(Issue.createdAtField) shouldBe a[java.util.Date]
    struct.get(Issue.updatedAtField) shouldBe a[java.util.Date]
    struct.get(Issue.urlField) shouldBe "https://api.github.com/repos/apache/kafka/issues/2800"
    struct.get(Issue.htmlUrlField) shouldBe "https://github.com/apache/kafka/pull/2800"
    struct.get(Issue.titleField) shouldBe a[String]
    struct.get(Issue.stateField) shouldBe "closed"
    val userStruct = struct.getStruct(Issue.userField)
    userStruct should not be null
    userStruct.get(User.loginField) shouldBe "simplesteph"
    userStruct.get(User.idField) shouldBe 20851561
    userStruct.get(User.urlField) shouldBe "https://api.github.com/users/simplesteph"
    userStruct.get(User.htmlUrlField) shouldBe "https://github.com/simplesteph"
    val prStruct = struct.getStruct(Issue.pullRequestField)
    prStruct should not be null
    prStruct.get(PullRequest.urlField) shouldBe "https://api.github.com/repos/apache/kafka/pulls/2800"
    prStruct.get(PullRequest.htmlUrlField) shouldBe "https://github.com/apache/kafka/pull/2800"
  }

  private def validateIssue(issue: Issue) = {
    import issue._
    url shouldBe "https://api.github.com/repos/apache/kafka/issues/2800"
    htmlUrl shouldBe "https://github.com/apache/kafka/pull/2800"
    title shouldBe "added interface to allow producers to create a ProducerRecord without…"
    createdAt.toString shouldBe "2017-04-04T06:47:09Z"
    updatedAt.toString shouldBe "2017-04-19T22:36:21Z"
    number shouldBe 2800
    state shouldBe "closed"
    validateUser(issue.user)
    issue.pullRequest should not be empty
    validatePullRequest(issue.pullRequest.get)
  }

  private def validateUser(user: User) = {
    import user._
    id shouldBe 20851561
    url shouldBe "https://api.github.com/users/simplesteph"
    htmlUrl shouldBe "https://github.com/simplesteph"
    login shouldBe "simplesteph"
  }

  private def validatePullRequest(pullRequest: PullRequest) = {
    import pullRequest._
    url shouldBe "https://api.github.com/repos/apache/kafka/pulls/2800"
    htmlUrl shouldBe "https://github.com/apache/kafka/pull/2800"
  }

}
