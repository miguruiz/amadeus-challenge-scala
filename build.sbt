name := "amadeus-challenge-scala"

version := "0.1"

scalaVersion := "2.12.8"

//Spark
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.2" 
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.2"

//Scalatest
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.8"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"


libraryDependencies +="com.holdenkarau" %% "spark-testing-base" % "2.4.2_0.12.0" % "test"