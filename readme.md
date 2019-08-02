# Kafka Real-World Exercises

These are examples from **Stephane Maarek**'s online courses on Kafka. 
See Udemy website or https://www.kafka-tutorials.com for them. 

* Producer and Consumer from **Kafka beginners v2** course. 
  * Course at Udemy: https://www.udemy.com/course/apache-kafka/
  * Original java solution by **Stephane Maarek**: https://github.com/simplesteph/kafka-beginners-course

* Custom connector from **Kafka Connect Hands-on Learning** course. 
  * Course at Udemy: https://www.udemy.com/kafka-connect/
  * Original java solution by **Stephane Maarek**: https://github.com/simplesteph/kafka-connect-github-source
  
## Twitter Producer

_The Twitter Producer gets data from Twitter based on some keywords and put them 
in a Kafka topic of your choice._  Here are some pointers for the exercise:

* Implementation: [TwitterProducerApp](src/main/scala/example/client/TwitterProducerApp.scala)

* Twitter Java Client: https://github.com/twitter/hbc

* Twitter API&nbsp;Credentials: https://developer.twitter.com/

* Original java solution by **Stephane Maarek**: 
  https://github.com/simplesteph/kafka-beginners-course/tree/master/kafka-producer-twitter

## ElasticSearch Consumer

_The ElasticSearch Consumer gets data from your twitter topic and inserts it into ElasticSearch._ 
Here are some pointers for the exercise:

* Implementation [ElasticConsumerApp](src/main/scala/example/client/ElasticConsumerApp.scala)

* ElasticSearch Java Client https://www.elastic.co/guide/en/elasticsearch/client/java-rest/6.4/java-rest-high.html

* ElasticSearch setup
 
    * https://www.elastic.co/guide/en/elasticsearch/reference/current/setup.html

    * OR https://bonsai.io/
    
* Original java solution by **Stephane Maarek**:
  https://github.com/simplesteph/kafka-beginners-course/tree/master/kafka-consumer-elasticsearch

## GitHub Connector
 _Custom Kafka connector that gets a stream of issues and pull requests for your chosen GitHub repository._
 
 * Implementation
 
   * Connector class: [GitHubSourceConnector](src/main/scala/example/connector/GitHubSourceConnector.scala)
 
   * Most important class: [GitHubSourceTask](src/main/scala/example/connector/GitHubSourceTask.scala)
   
   * Script to run Landoop (Lenses) `fast-data-dev` Kafka cluster with our custom connector mounted:
     [run-docker-with-connector.sh](run-docker-with-connector.sh)
  
 * Original java solution by **Stephane Maarek**: https://github.com/simplesteph/kafka-connect-github-source
 