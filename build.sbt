name := """tryplay"""

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  jdbc,
  anorm,
  cache,
  ws,
"org.apache.spark" %% "spark-core" % "1.2.0",
"com.datastax.spark" %% "spark-cassandra-connector" % "1.1.0",
"org.apache.spark" %% "spark-streaming" % "1.2.0",
"org.apache.spark" %% "spark-streaming-kafka" % "1.2.0",
"com.typesafe.akka" %% "akka-actor" % "2.2.3",
"com.typesafe.akka" %% "akka-slf4j" % "2.2.3"
)

