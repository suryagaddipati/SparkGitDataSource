import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.12.7",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "SparkGitDataSource",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0",
    libraryDependencies += "com.madgag.scala-git" %% "scala-git" % "4.0",
    libraryDependencies += "co.fs2" %% "fs2-core" % "1.0.1" ,
    libraryDependencies += scalaTest % Test
  )
