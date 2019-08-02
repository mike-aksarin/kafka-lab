package example.connector

import java.time.Instant
import java.time.format.DateTimeParseException

import org.apache.kafka.common.config.{ConfigDef, ConfigException}

class TimestampValidator extends ConfigDef.Validator {

  override def ensureValid(name: String, value: AnyRef): Unit = {
    try
      Instant.parse(value.toString)
    catch {
      case e: DateTimeParseException =>
        throw new ConfigException(name, value, s"$validationMessage $e")
    }
  }

  def validationMessage: String =
     "Wasn't able to parse the timestamp, make sure it is formatted according to ISO-8601 standards."

}
