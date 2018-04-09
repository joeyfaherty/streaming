package structuredstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Dataset, KeyValueGroupedDataset, SparkSession}

import scala.util.Try

/**
  * run: nc -lk 9999
  *
  * session1,100
  * session2,200
  *
  *
-------------------------------------------
Batch: 0
-------------------------------------------
+--------+--------+-------+
|      id|totalSum|expired|
+--------+--------+-------+
|session1|   100.0|  false|
|session2|   200.0|  false|
+--------+--------+-------+
  *
  * session1,200
  *
  * -------------------------------------------
Batch: 1
-------------------------------------------
+--------+--------+-------+
|      id|totalSum|expired|
+--------+--------+-------+
|session1|   300.0|  false|
+--------+--------+-------+
  *
  * session1,200,end
  *
  * -------------------------------------------
Batch: 2
-------------------------------------------
+--------+--------+-------+
|      id|totalSum|expired|
+--------+--------+-------+
|session1|   500.0|   true|
+--------+--------+-------+
  *
  * session1,100
  * session2,200
  *
  * -------------------------------------------
Batch: 3
-------------------------------------------
+--------+--------+-------+
|      id|totalSum|expired|
+--------+--------+-------+
|session1|   100.0|  false|
|session2|   400.0|  false|
+--------+--------+-------+
  *
  * From the output you can observe that session1 is started from scratch and session2 is updated.
  */
object SessionWindowUsingACustomerState {

  val exampleJson = """"{"id":"A1234", "email":"blahblah@hotmail.com"}"""

  case class Customer(id: String, emailAddress: String)

  case class Session(sessionId: String, value: Double, endSignal: Option[String])

  case class SessionInfo(totalSum: Double)

  case class SessionUpdate(id: String, totalSum: Double, expired: Boolean)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession
      .builder
      // run app locally utilizing all cores
      .master("local[*]")
      .appName(getClass.getName)
      .getOrCreate()

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val stream: DataFrame = spark.readStream
      // socket input data source format
      .format("socket")
      // connect to socket port localhost:9999 waiting for incoming stream
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    import spark.implicits._
    val socketDs: Dataset[String] = stream.as[String]
    // events
    val events: Dataset[Session] = socketDs.map(line => {
      val columns = line.split(",")
      val endSignal = Try(Some(columns(2))).getOrElse(None)
      Session(columns(0), columns(1).toDouble, endSignal)
    })

    // create a DS of kv pairs (sessionId, Session)
    val idSessionKv: KeyValueGroupedDataset[String, Session] = events.groupByKey(x => x.sessionId)


    /*
     * mapGroupsWithState[SessionInfo, SessionUpdate] takes two models:
     * SessionInfo, which is the state that we are tracking
     * SessionUpdate,  which is the return value of the function
     *
     * We use GroupStateTimeout.NoTimeout() to indicate no timeout as we want the session to close after explicit user input.
     * Other timeouts can also be specified, for example if you wish to close state after this timeout
     */
    val sessionUpdates: Dataset[SessionUpdate] = idSessionKv.mapGroupsWithState[SessionInfo, SessionUpdate](GroupStateTimeout.NoTimeout()) {
      // mapGroupsWithState: key: K, it: Iterator[V], s: GroupState[S]
      case (sessionId: String, eventsIter: Iterator[Session], state: GroupState[SessionInfo]) => {
        val events = eventsIter.toSeq
        val updatedSession =
          if (state.exists) {
            val existingState = state.get
            val updatedEvents = SessionInfo(existingState.totalSum + events.map(_.value).sum)
            updatedEvents
          }
          else {
            SessionInfo(events.map(event => event.value).sum)
          }

        state.update(updatedSession)

        val isEndSignal = events.exists(_.endSignal.isDefined)
        if (isEndSignal) {
          state.remove()
          SessionUpdate(sessionId, updatedSession.totalSum, expired = true)
        }
        else {
          SessionUpdate(sessionId, updatedSession.totalSum, expired = false)
        }
      }
    }

    val query: StreamingQuery = sessionUpdates
      .writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()

  }


}