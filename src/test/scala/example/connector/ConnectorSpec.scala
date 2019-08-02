package example.connector

import org.scalatest.{FlatSpec, Matchers}

class ConnectorSpec extends FlatSpec with Matchers {

  it should "return single task config" in {
    val connector = new GitHubSourceConnector()
    connector.start(TestConfig.props)
    connector.taskConfigs(maxTasks = 1) should have size 1
    connector.taskConfigs(maxTasks = 10) should have size 1
  }

}
