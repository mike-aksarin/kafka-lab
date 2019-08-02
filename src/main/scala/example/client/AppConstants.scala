package example.client

import java.time.Duration

object AppConstants {

  val bootstrapServers: String = "127.0.0.1:9092"
  val topic: String = "tweets"
  val twitterPollTimeout = 500 //ms
  val groupId: String = "kafka-beginners-app"
  val kafkaPollTimeout: Duration = Duration.ofMillis(100) // new in Kafka 2.0.0
  val index: String = topic

}
