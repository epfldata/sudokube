import Dependencies._

ThisBuild / scalaVersion := "2.12.7"
ThisBuild / version := "0.2.0"
ThisBuild / organization := "ch.epfl.data"
ThisBuild / organizationName := "data"

lazy val root = (project in file("."))
  .settings(
    name := "sudokube",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "com.github.sbt" % "junit-interface" % "0.13.2" % Test)
  .settings(javah / target := sourceDirectory.value / "native")
  .dependsOn(originalCBackend % Runtime)
  .dependsOn(rowStoreCBackend % Runtime)
  .dependsOn(colStoreCBackend % Runtime)
  .dependsOn(trieStoreCBackend % Runtime)
  .aggregate(originalCBackend, rowStoreCBackend, colStoreCBackend, trieStoreCBackend)


Test / parallelExecution := false

libraryDependencies ++= Seq(
  "org.scalanlp" %% "breeze" % "0.13.2",
  "org.scalanlp" %% "breeze-natives" % "0.13.2",
  "org.scalanlp" %% "breeze-viz" % "0.13.2",
  "org.scala-lang.modules" %% "scala-swing" % "2.1.1",
  "org.apache.commons" % "commons-lang3" % "3.12.0",
  //"org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.5.3",
  "com.github.tototoshi" %% "scala-csv" % "1.3.10",
  "org.apache.xmlgraphics" % "batik-all" % "1.16",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.8.8")

lazy val originalCBackend = (project in file("src") / "native" / "Original")
  .settings(nativeCompile / sourceDirectory := baseDirectory.value)
  .settings(nativeCompile / target :=  target.value)
  .enablePlugins(JniNative)

lazy val rowStoreCBackend = (project in file("src") / "native" / "RowStore")
  .settings(nativeCompile / sourceDirectory := baseDirectory.value)
  .settings(nativeCompile / target :=  target.value)
  .enablePlugins(JniNative)

lazy val colStoreCBackend = (project in file("src") / "native" / "ColumnStore")
  .settings(nativeCompile / sourceDirectory := baseDirectory.value)
  .settings(nativeCompile / target := target.value)
  .enablePlugins(JniNative)

lazy val trieStoreCBackend = (project in file("src") / "native" / "TrieStore")
  .settings(nativeCompile / sourceDirectory := baseDirectory.value)
  .settings(nativeCompile / target := target.value)
  .enablePlugins(JniNative)

enablePlugins(JavaAppPackaging)

resolvers ++= Seq(
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)

scalaVersion := "2.12.7"

