package example.connector

import org.apache.kafka.common.config.{ConfigDef, ConfigException}

class BatchSizeValidator(maxBatchSize: Int) extends ConfigDef.Validator {

  override def ensureValid(name: String, value: AnyRef): Unit = value match {
    case batchSize: java.lang.Integer if isValid(batchSize) => () // Ok
    case _ => throw new ConfigException(name, value, validationMessage)
  }

  def isValid(batchSize: Int): Boolean =
    batchSize > 0 && batchSize <= maxBatchSize

  def validationMessage: String =
    s"Batch Size must be a positive integer that's less or equal to $maxBatchSize"

}
