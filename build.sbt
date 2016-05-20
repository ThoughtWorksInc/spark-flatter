scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" % Provided

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" % Test

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1" % Provided

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1" % Test

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0-M15" % Test

libraryDependencies += "com.jsuereth" %% "scala-arm" % "1.4" % Test

libraryDependencies += "com.dongxiguo" %% "fastring" % "0.2.4"