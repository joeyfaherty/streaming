package structuredstreaming.customsinks.rest

import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import scalaj.http.Http


case class RestApiSink(sqlContext: SQLContext,
                       options: Map[String, String],
                       partitionColumns: Seq[String],
                       outputMode: OutputMode) extends Sink {

  override def addBatch(batchId: Long, data: DataFrame): Unit = synchronized {
    // use a local variable to make sure the map closure doesn't capture the whole DataFrame
    val schema = data.schema
    val rdd = data.queryExecution.toRdd.mapPartitions { rows =>
      val converter = CatalystTypeConverters.createToScalaConverter(schema)
      rows
        .map(converter(_).asInstanceOf[Row].getAs[String](0))
    }

    rdd.foreach(output => {
      send(output, options)
    })

  }

  def send(json: String, options: Map[String, String]): Unit = {
    val postUrl = options("post_url")

    val response = Http(postUrl)
      .method("post")
      .header("Content-Type", "application/json")
      .postData(json)
      .asString
      .body

    println(s"Posting to Rest API complete, response is: ${response}")
  }

}

class RestApiSinkProvider extends StreamSinkProvider with DataSourceRegister {

  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {

    RestApiSink(sqlContext, parameters, partitionColumns, outputMode)
  }

  override def shortName(): String = "custom-rest-api-sink"
}
