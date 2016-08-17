crossScalaVersions in ThisBuild := Seq("2.10.6", "2.11.8")

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" % Provided

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" % Test

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1" % Provided

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1" % Test

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0-M15" % Test

libraryDependencies += "com.jsuereth" %% "scala-arm" % "1.4" % Test

libraryDependencies += "com.dongxiguo" %% "fastring" % "0.2.4"

organization in ThisBuild := "com.thoughtworks.spark-flatter"



libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

libraryDependencies += "org.scala-lang" % "scala-compiler" % scalaVersion.value % Test

libraryDependencies ++= {
  if (scalaBinaryVersion.value == "2.10") {
    Seq("org.scalamacros" %% "quasiquotes" % "2.1.0")
  } else {
    Seq()
  }
}