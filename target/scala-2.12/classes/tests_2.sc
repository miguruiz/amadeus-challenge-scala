
val arg_0 = "0"
val arg_1 = "1"

val bookingsFilePath = {
  if (arg_0.contains("booking"))
    arg_0
  else if (arg_1.contains("booking"))
    arg_1
  else
    "Error"
}

val searchesFilePath = {
  if (arg_0.contains("searches"))
    arg_0
  else if (arg_1.contains("searches"))
    arg_1
  else
    "Error"
}

if (bookingsFilePath == "Error" || searchesFilePath == "Erro")
  println("Unable to identify file names")
  //return
else
  //DataQuality
  // Bookings -> not empty, columns, X,Y,Z
  // Searches -> not empty, columns, X,Y,Z
