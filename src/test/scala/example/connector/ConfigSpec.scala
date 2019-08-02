package example.connector

import java.time.Instant

import example.connector.GitHubConnectorConfig._
import org.apache.kafka.common.config.ConfigValue
import org.scalatest.{FlatSpec, Inspectors, Matchers}

import scala.collection.JavaConverters._

class ConfigSpec extends FlatSpec with Matchers with Inspectors {

  it should "format into reStructuredText" in {
    val formatted = configDef.toRst
    formatted should not be empty
    println(formatted)
  }

  it should "pass correct properties validation" in {
    //configDef.validate returns list
    forAll (configDef.validate(TestConfig.props)) { configValue =>
      configValue.errorMessages shouldBe empty
    }
  }

  it should "read config" in {
    val config = new GitHubConnectorConfig(TestConfig.props)
    config.batchSize shouldBe TestConfig.srcMap(GitHubConnectorConfig.batchSizeProperty).toInt
    config.sinceTimestamp shouldBe an[Instant]
    config.authUser shouldBe empty
  }

  it should "not allow malformed `since` timestamp" in {
    configValue(sinceTimestampProperty -> "not a date").errorMessages should have size 1
  }

  it should "not allow negative batch size" in {
    configValue(batchSizeProperty -> "-1").errorMessages should have size 1
  }

  it should "not allow batch size > 100" in {
    configValue(batchSizeProperty -> "101").errorMessages should have size 1
  }

  it should "pass auth user validation" in {
    configValue(authUserProperty -> "simplesteph").errorMessages shouldBe empty
  }

  it should "pass auth password validation" in {
    configValue(authPasswordProperty -> "kafka is awesome").errorMessages shouldBe empty
  }

  def configValue(property: (String, String)): ConfigValue = {
    //configDef.validateAll returns map
    val configValueMap = configDef.validateAll((TestConfig.srcMap + property).asJava)
    configValueMap.get(property._1)
  }

}
