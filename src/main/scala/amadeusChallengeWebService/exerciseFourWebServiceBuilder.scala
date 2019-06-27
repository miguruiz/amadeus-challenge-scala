/**
  * Name: exerciseFourWebServiceBuilder.scala
  *
  * Description:
  *   Web service builder
  *
  */
package amadeusChallengeWebService

import javax.servlet.Servlet
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.webapp.WebAppContext

object exerciseFourWebServiceBuilder {


  def buildWebSErvice(port: Int, webServiceClass: Class [_<: Servlet]) = {

    val server: Server = new Server(port)
    val context: WebAppContext= new WebAppContext()


    context.setContextPath("/")
    context.setResourceBase("/temp")
    context.addServlet(webServiceClass, "/*" )
    server.setHandler(context)
    server
  }

}
