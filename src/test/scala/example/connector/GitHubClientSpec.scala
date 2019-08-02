package example.connector

import java.time.Instant

import org.scalatest.{FlatSpec, Matchers}

class GitHubClientSpec extends FlatSpec with Matchers {
  val client = new GitHubClient(TestConfig.config)

  it should "query issues" in {
    val nextPage = 1
    val timestamp = Instant.parse("2017-01-01T00:00:00Z")
    val url = client.issuesUrl(nextPage, timestamp)
    println(url)
    val response = client.sendRequest(url)
    println(response.getBody)
    response.getStatus shouldBe 200
    val headers = response.getHeaders
    headers.get("ETag") should not be empty
    headers.get("X-RateLimit-Limit") should not be empty
    headers.get("X-RateLimit-Remaining") should not be empty
    val jsonArray = response.getBody.getArray
    jsonArray.length shouldBe TestConfig.config.batchSize
    val issue = GitHubClient.parseIssue(jsonArray.getJSONObject(0))
    issue.number should be > 0
    issue.number shouldBe 2072
  }

}
