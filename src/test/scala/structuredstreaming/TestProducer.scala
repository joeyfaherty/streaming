package structuredstreaming

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object TestProducer {

  import scala.collection.JavaConversions._

  def apply(urls: String): TestProducer = {
    val f = () => {
      val kafkaConfig: Map[String, Object] = Map(
        "bootstrap.servers" -> urls,
        "retries" -> 3,
        "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
        "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
      )

      val producer = new KafkaProducer[String, String](kafkaConfig)
      sys.addShutdownHook {
        producer.close()
      }
      producer
    }

    new TestProducer(f)
  }
}

class TestProducer(createProducer: () => KafkaProducer[String, String]) extends Serializable {

  val producer = createProducer()

  def send(topic: String, key: String = System.currentTimeMillis().toString, value: String): Unit = {
    producer.send(new ProducerRecord(topic, key, value))
  }

}
