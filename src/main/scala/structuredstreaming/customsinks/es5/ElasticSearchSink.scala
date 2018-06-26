package structuredstreaming.customsinks.es5

import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.elasticsearch.spark.rdd.EsSpark

case class ElasticSearchSink(sqlContext: SQLContext,
                             options: Map[String, String],
                             partitionColumns: Seq[String],
                             outputMode: OutputMode)
  extends Sink {

  override def addBatch(batchId: Long, data: DataFrame): Unit = synchronized {
    // use a local variable to make sure the map closure doesn't capture the whole DataFrame
    val schema = data.schema
    // this ensures that the same query plan will be used
    val rdd = data.queryExecution.toRdd.mapPartitions {
      rows =>
        val converter = CatalystTypeConverters.createToScalaConverter(schema)
        rows.map(converter(_).asInstanceOf[Row])
          // expects a single json string event for each row
          .map(_.getAs[String](0))
    }

    EsSpark.saveJsonToEs(
      rdd,
      options("es.resource.write"), // the es index to write to
      Map("es.mapping.id" -> options("es.mapping.id")) // the mapping id
    )
  }
}

/**
  * ElasticSearch 5.x compatible sink that can be used in the writeStream.
  * Use the path to this class 'ElasticSearchSinkProvider' in the .format() to use it.
  *
  * If using ElasticSearch 6.x, there is a Sink supplied out of the box so no need to use a custom sink.
  */
class ElasticSearchSinkProvider extends StreamSinkProvider with DataSourceRegister {

  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {

    ElasticSearchSink(sqlContext, parameters, partitionColumns, outputMode)
  }

  override def shortName(): String = "custom-es-sink"
}
