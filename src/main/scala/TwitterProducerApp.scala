import java.util.Properties
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import com.google.common.collect.Lists
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.event.Event
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.core.{Constants, HttpHosts}
import com.twitter.hbc.httpclient.BasicClient
import com.twitter.hbc.httpclient.auth.OAuth1
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

/** This is just an example kafka producer app.
  * Note that there are also Twitter source connectors for Kafka, which doesn't require
  * a single line of code to pull data from Twitter into Kafka.
  * For example you can use [[https://github.com/jcustenborder/kafka-connect-twitter]]
  */
object TwitterProducerApp extends App {

  val log = LoggerFactory.getLogger("producer-app")
  val twitterMsgQueue = new LinkedBlockingQueue[String](1000)
  val twitterEventQueue = new LinkedBlockingQueue[Event](1000)

  /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
  val twitterHosts = new HttpHosts(Constants.STREAM_HOST)
  val twitterEndpoint = createTwitterEndpoint()

  private val consumerKey = sys.env("consumerKey")
  private val consumerSecret = sys.env("consumerSecret")
  private val token = sys.env("token")
  private val secret = sys.env("secret")

  // These secrets should be read from a config file
  val twitterAuth = new OAuth1(consumerKey, consumerSecret, token, secret)
  val twitterClient = createTwitterClient()

  lazy val kafkaProducer: KafkaProducer[String, String] = createKafkaProducer()

  private def createKafkaProducer(): KafkaProducer[String, String] = {
    // create Producer properties
    val props: Properties = new Properties
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConstants.bootstrapServers)
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    // high throughput producer
    props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true.toString) // Prevent duplicates and produce exactly once
    props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy") // Works well for the huge batches (snappy, lz4)
    props.setProperty(ProducerConfig.LINGER_MS_CONFIG, 20.toString) // Wait before sending a batch out
    props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, (64 * 1024).toString) // Maximum number of bytes in a batch
    new KafkaProducer[String, String](props) // create the producer
  }

  private def sendRecord(message: String) = {
    // create a producer record
    val record = new ProducerRecord[String, String](AppConstants.topic, message)
    kafkaProducer.send(record) // send data - asynchronous
  }

  private def closeKafka(): Unit = {
    kafkaProducer.flush() // flush data
    kafkaProducer.close() // flush and close producer
  }

  private def createTwitterEndpoint(): StatusesFilterEndpoint = {
    val hosebirdEndpoint = new StatusesFilterEndpoint()
    // Optional: set up some followings and track terms
    val terms = Lists.newArrayList("eirene", "peace")
    val followings = Lists.newArrayList(java.lang.Long.valueOf(2981206113L))
    hosebirdEndpoint.followings(followings)
    hosebirdEndpoint.trackTerms(terms)
  }

  private def createTwitterClient(): BasicClient = {
    val builder = new ClientBuilder()
      .name("Hosebird-Client-01") // optional: mainly for the logs
      .hosts(twitterHosts)
      .authentication(twitterAuth)
      .endpoint(twitterEndpoint)
      .processor(new StringDelimitedProcessor(twitterMsgQueue))
      .eventMessageQueue(twitterEventQueue) // optional: use this if you want to process client events
    builder.build
  }

  private def closeTwitter(): Unit = {
    twitterClient.stop()
  }

  private def run(): Unit = {
    log.info("Connecting to twitter")
    twitterClient.connect() // Attempts to establish a connection
    try {
      while (!twitterClient.isDone) {
        val maybeMsg = Option(twitterMsgQueue.poll(AppConstants.twitterPollTimeout, TimeUnit.MILLISECONDS))
        maybeMsg.foreach { msg =>
          log.info(msg)
          sendRecord(msg)
        }
      }
    } catch {
      case e: InterruptedException => log.error("Application got interrupted", e)
    } finally {
      log.info("Application is closing")
      closeTwitter()
      closeKafka()
    }
  }

  run()

}
