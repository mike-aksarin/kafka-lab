name := "kafka-lab"

version := "0.1"

scalaVersion := "2.12.8"

val kafkaVersion = "2.2.1"
val twitterApiVersion = "2.2.0"
val elasticApiVersion = "7.2.0"

scalacOptions += "-Ydebug"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "com.twitter" % "hbc-core" % twitterApiVersion,
  "org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % elasticApiVersion,
  "org.slf4j" % "slf4j-simple" % "1.7.26"
)