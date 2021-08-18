import Dependencies._

ThisBuild / scalaVersion     := "2.12.7"
ThisBuild / version          := "0.2.0"
ThisBuild / organization     := "ch.epfl.data"
ThisBuild / organizationName := "data"

lazy val root = (project in file("."))
  .settings(
    name := "sudokube",
    libraryDependencies += scalaTest % Test
  )

libraryDependencies  ++= Seq(
  "org.scalanlp" %% "breeze" % "0.13.2",
  "org.scalanlp" %% "breeze-natives" % "0.13.2",
  "org.scalanlp" %% "breeze-viz" % "0.13.2",
  "org.scala-lang.modules" %% "scala-swing" % "2.1.1",
  //"org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.5.3",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.8.8"
)

Test / parallelExecution := false

enablePlugins(JavaAppPackaging)

resolvers ++= Seq(
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)

scalaVersion := "2.12.7"

