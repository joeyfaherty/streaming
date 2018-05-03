package structuredstreaming

object TestSend {

  val producer = TestProducer.apply("localhost:9092")

  val topic = "customer-topic"
  val key = "mykey"

  val customer1 = "id_12345,john,swan,16"
  val customer2 = "id_12346,james,duck,25"

  def main(args: Array[String]): Unit = {
    producer.send(topic, key, customer1)
    producer.send(topic, key, customer2)
  }

}
