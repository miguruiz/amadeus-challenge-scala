package amadeusChallengeWebService

class exerciseFourWebService extends ScalatraServlet{

  get("/") {
    "Scalatra rules!"
  }

  get("/temp") {
    "You are inside temp!"
  }

}
