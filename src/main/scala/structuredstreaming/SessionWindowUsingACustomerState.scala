package structuredstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Dataset, KeyValueGroupedDataset, SparkSession}

import scala.util.Try

object SessionWindowUsingACustomerState {

  val sessionJson =
    """
      |{
      |  "sessionId": "session1",
      |  "winAmount": 100,
      |  "deposit": false
      |}
      |""".stripMargin

  case class Transaction(sessionId: String, winAmount: Double, deposit: Boolean)

  case class SessionTrackingValue(totalSum: Double)

  case class SessionUpdate(sessionId: String, currentBalance: Double, depositCurrentBalance: Boolean)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession
      .builder
      // run app locally utilizing all cores
      .master("local[*]")
      .appName(getClass.getName)
      .getOrCreate()

    /*
     * From a terminal, run:
     *
     * nc -lc 9999
     *
     * as input for the socket stream
     */
    val socketStream: DataFrame = spark.readStream
      // socket as stream input
      .format("socket")
      // connect to socket port localhost:9999 waiting for incoming stream
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    import spark.implicits._

    val transactions = socketStream
      .as[String]
      .map(inputLine => {
      val fields = inputLine.split(",")
      Transaction(fields(0), fields(1).toDouble, Try(fields(2).toBoolean).getOrElse(false))
    })

    // create a DS of kv pairs (sessionId, Transaction)
    val idSessionKv: KeyValueGroupedDataset[String, Transaction] = transactions.groupByKey(x => x.sessionId)


    /*
     * mapGroupsWithState[SessionInfo, SessionUpdate] takes two models:
     * SessionInfo, which is the state that we are tracking
     * SessionUpdate,  which is the return value of the function
     *
     * We use GroupStateTimeout.NoTimeout() to indicate no timeout as we want the session to close after explicit user input.
     * Other timeouts can also be specified, for example if you wish to close state after this timeout
     */
    val sessionUpdates: Dataset[SessionUpdate] = idSessionKv.mapGroupsWithState[SessionTrackingValue, SessionUpdate](GroupStateTimeout.NoTimeout()) {
      // mapGroupsWithState: key: K, it: Iterator[V], s: GroupState[S]
      case (sessionId: String, eventsIter: Iterator[Transaction], state: GroupState[SessionTrackingValue]) => {
        val events = eventsIter.toSeq
        val updatedSession =
          if (state.exists) {
            val existingState = state.get
            val updatedEvents = SessionTrackingValue(existingState.totalSum + events.map(_.winAmount).sum)
            updatedEvents
          }
          else {
            SessionTrackingValue(events.map(event => event.winAmount).sum)
          }

        state.update(updatedSession)

        val toCloseSession = events.exists(_.deposit)

        // when there is an deposit in the event, close the session by removing the state
        if (toCloseSession) {
          // here we could perform a specific action when we receive the end of the session signal (store, send, update other state)
          // in this case we would just deposit the current balance to a data store
          // state.save() .. TODO unimplemented for this example
          state.remove()
          SessionUpdate(sessionId, updatedSession.totalSum, depositCurrentBalance = true)
        }
        else {
          SessionUpdate(sessionId, updatedSession.totalSum, depositCurrentBalance = false)
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