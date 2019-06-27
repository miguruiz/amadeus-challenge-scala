/**
  * Name: Main.scala  - Bonus questions: write a Web Service
  *
  * Description:
  *   Wrap the output of the second exercise in a web service that returns the data in JSON format
  *   (instead of printing to the standard output). The web service should accept a parameter n>0. For the top
  *   10 airports, n is 10. For the X top airports, n is X.
  *
  */

package amadeusChallengeWebService

object Main extends App{


  val server = exerciseFourWebServiceBuilder.buildWebSErvice(8080,classOf[exerciseFourWebService])
  server.start()

}
