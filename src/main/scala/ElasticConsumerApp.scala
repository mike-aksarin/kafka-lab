import java.util
import java.util.Properties
import java.util.concurrent.CountDownLatch

import org.apache.http.HttpHost
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.elasticsearch.action.bulk.{BulkRequest, BulkResponse}
import org.elasticsearch.action.{ActionListener, DocWriteRequest}
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters


/** This is an example kafka consumer app. Please note that Kafka has connectors
  * for the Elastic Search, which enables integration without
  * a single line of code to push data to Elastic Search from Kafka.
  * You can check the result of an app via Kafka rest API: [[http://localhost:9200/tweets/]].
  * All the indices are at [[http://localhost:9200/_cat/indices?v]].
  */
object ElasticConsumerApp extends App with Runnable with ActionListener[IndexResponse] {

  val log = LoggerFactory.getLogger("consumer-app")
  val latch: CountDownLatch = new CountDownLatch(1) // latch for dealing with multiple threads
  val consumer: KafkaConsumer[String, String] = createKafkaConsumer()
  val elasticClient: RestHighLevelClient = createElasticClient()
  val pollsterThread = new Thread(this)

  def start(): Unit = {
    log.info("Starting pollster thread")
    subscribe()
    pollsterThread.start()
    Runtime.getRuntime.addShutdownHook(new Thread(() => shutdown()))
    try {
      latch.await()
    } catch {
      case e: InterruptedException => log.error("Application got interrupted", e)
    } finally {
      log.info("Application is closing")
    }
  }

  def shutdown(): Unit = {
    log.info("Caught shutdown hook")
    close()
    try latch.await() catch {
      case e: InterruptedException => log.info("Interrupted", e)
    }
    log.info("Application has exited")
  }

  private def createKafkaConsumer() = {
    // CSSreate consumer configs
    val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConstants.bootstrapServers)
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, AppConstants.groupId)

    // Read data from the very beginning ("latest", "earliest", "none")
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    // Disable auto-commit of consumer offsets
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false.toString)

    // Maximum records to receive during one poll
    //properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10)

    new KafkaConsumer[String, String](properties) // create consumer
  }

  def subscribe(): Unit = {
    // subscribe consumer to our topic(s)
    consumer.subscribe(util.Arrays.asList(AppConstants.topic))
  }

  def run(): Unit = {
    try {
      while (true) poll()
    } catch {
      case _: WakeupException => log.info("Received shutdown signal!")
    } finally {
      consumer.close()
      latch.countDown()
    }
  }

  def poll(): ConsumerRecords[String, String] = {
    val records: ConsumerRecords[String, String] = consumer.poll(AppConstants.kafkaPollTimeout)
    if (!records.isEmpty) {
      val messages = for {
        record <- JavaConverters.iterableAsScalaIterable(records)
        message <- Option(record.value())
      } yield {
        log.info(s"Key: ${record.key}, Value: ${record.value}") // record.key is null here
        log.info(s"Topic: ${record.topic}, Partition: ${record.partition}, Offset:${record.offset}")
        val id = s"${record.topic}_${record.partition}_${record.offset}" //Kafka generic id
        id -> message
      }
      if (messages.nonEmpty) {
        sendRecords(messages)
      }
    }
    records
  }

  def close(): Unit = {
    // the wakeup() method is a special method to interrupt consumer.poll()
    // it will throw the exception WakeUpException
    consumer.wakeup()
    elasticClient.close()
  }

  private def createElasticClient(): RestHighLevelClient = {
    val builder = RestClient.builder(new HttpHost("localhost", 9200, "http"))
    new RestHighLevelClient(builder)
  }

  def sendRecords(records: Iterable[(String, String)]): BulkResponse = {
    val request: BulkRequest = records.foldLeft(new BulkRequest()) {
      case (bulk, (key, msg)) =>
        val item = new IndexRequest(AppConstants.index)
          .id(key) // made the consumer idempotent
          .source(msg, XContentType.JSON)
          .opType(DocWriteRequest.OpType.CREATE)
        //elasticClient.index(item, RequestOptions.DEFAULT) // Send requests one by one
        bulk.add(item)
    }
    val result = elasticClient.bulk(request, RequestOptions.DEFAULT)
    consumer.commitSync() //commit the consumer offset
    result
  }

  def onResponse(response: IndexResponse): Unit = {
    val info = response.getShardInfo
    log.info(s"Inserted index with id ${response.getId} ${response.getResult}. " +
             s"Successful: ${info.getSuccessful}, failed: ${info.getFailed}, total: ${info.getTotal} ")
  }

  def onFailure(e: Exception): Unit = {
    log.error("Failed sending index element", e)
  }

  start()

}
