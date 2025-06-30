ThisBuild / scalaVersion := "3.7.1"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.codecrafters"
ThisBuild / organizationName := "CodeCrafters"

assembly / assemblyJarName := "redis.jar"

import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.MergeStrategy

assembly / assemblyMergeStrategy := {
  case "module-info.class" => MergeStrategy.discard
  case x                   => (assembly / assemblyMergeStrategy).value(x)
}

resolvers += "Akka library repository".at("https://repo.akka.io/maven")

val PekkoVersion = "1.1.4"

lazy val root = (project in file("."))
  .settings(
    name := "codecrafter-redis",
    // List your dependencies here
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      "com.github.scopt" %% "scopt" % "4.1.0",
      "ch.qos.logback" % "logback-classic" % "1.5.6",
      "org.apache.pekko" %% "pekko-actor-typed" % PekkoVersion,
      "org.apache.pekko" %% "pekko-stream" % PekkoVersion, // 'pekko-stream-typed' не существует, функциональность включена в 'pekko-stream'
      "org.apache.pekko" %% "pekko-slf4j" % PekkoVersion
    )
  )
