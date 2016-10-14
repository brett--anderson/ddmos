name := "didymus"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.1.0",
  "org.apache.kafka" % "kafka_2.10" % "0.10.0.1",
  "args4j" % "args4j" % "2.0.25"
)
