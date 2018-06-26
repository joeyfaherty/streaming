package structuredstreaming

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{Dataset, SparkSession}
import structuredstreaming.SimpleStructuredSreaming.Customer

class EsWriter5 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      // run app locally utilizing all cores
      .master("local[*]")
      .appName(getClass.getName)
      .config("es.nodes", "localhost")
      .config("es.port", "9200")
      .config("es.index.auto.create", "true") // this will ensure that index is also created on first POST
      .config("es.nodes.wan.only", "true") // needed to run against dockerized ES for local tests
      .getOrCreate()

    import spark.implicits._

    val customerEvents: Dataset[Customer] = spark
      .readStream
      .format("kafka")
      // maximum number of offsets processed per trigger interval
      .option("maxOffsetsPerTrigger", 100)
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "customer-topic")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()
      .select(
        $"key" cast "string", // deserialize binary key
        $"value" cast "string", // deserialize binary value
        $"topic",
        $"partition",
        $"offset"
      )
      .as[(String, String, String, String, String)]
      // convert kafka value to case class
      .map(x => {
      val split = x._2.split(",")
      val age = split(3).toInt
      Customer(split(0), split(1), split(2), age, age > 18, System.currentTimeMillis())
    })

    customerEvents
      .writeStream
      .outputMode(OutputMode.Append())
      .format("structuredstreaming.customsinks.es5.ElasticSearchSinkProvider")
      .option("checkpointLocation", "/checkpointLocation")
      .option("es.resource.write", "index/type")
      .option("es.mapping.id", "mappingColumn")
      .trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS))
      .start()
      .awaitTermination()

  }

}
