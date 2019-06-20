name := "amadeus-challenge-scala"

version := "0.1"

scalaVersion := "2.12.8"


libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.2" % "provided"



resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies += "com.github.xavierguihot" % "geobase" % "2.0.1"