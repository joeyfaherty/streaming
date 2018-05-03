package structuredstreaming

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec}

import scala.sys.process._

class DockerizedSparkStructuredStreamingE2e extends FlatSpec with BeforeAndAfterEach with BeforeAndAfterAll {


  /**
    * Bring up all containers before test suite execution
    */
  override def beforeAll = {
    "docker-compose up -d" !

    Thread.sleep(5000)
  }



  "When an event is sent to kafka the spark app" should "consume the event and write it to Elasticsearch" in {

    SimpleStructuredSreaming.main(Array())

  }

}
