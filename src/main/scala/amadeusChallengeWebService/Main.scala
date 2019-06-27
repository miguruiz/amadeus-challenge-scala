package amadeusChallengeWebService

object Main extends App{


  val server = exerciseFourWebServiceBuilder.buildWebSErvice(8080,classOf[exerciseFourWebService])
  server.start()

}
