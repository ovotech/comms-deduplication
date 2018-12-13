lazy val `consume-without-duplicate` = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.12.7",
      version := "1.0-SNAPSHOT"
    )),

    name := "consume-without-duplicate",

    scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      "-encoding",
      "utf8",
      "-target:jvm-1.8",
      "-feature",
      "-language:implicitConversions",
      "-language:higherKinds",
      "-language:existentials",
      "-Ypartial-unification",
      "-Ywarn-unused-import",
      "-Ywarn-value-discard"
    ),

    resolvers ++= Seq(
      "confluent-release" at "http://packages.confluent.io/maven/"
    ),

    libraryDependencies ++= Seq(
      "is.cir" %% "ciris-core" % "0.10.2",
      "is.cir" %% "ciris-cats" % "0.10.2",
      "is.cir" %% "ciris-cats-effect" % "0.10.2",
      "is.cir" %% "ciris-generic" % "0.10.2",
      "com.ovoenergy" %% "ciris-credstash" % "0.6",
      "com.ovoenergy" %% "ciris-aiven-kafka" % "0.6",
      "com.ovoenergy" %% "fs2-kafka" % "0.16.0",
      "com.ovoenergy" %% "kafka-serialization-core" % "0.3.17",
      "com.ovoenergy" %% "kafka-serialization-cats" % "0.3.17",
      "com.ovoenergy" %% "kafka-serialization-avro4s" % "0.3.17",
      "com.gu" %% "scanamo" % "1.0.0-M7",
      "com.gu" %% "scanamo-formats" % "1.0.0-M7",
      "com.ovoenergy" %% "comms-kafka-messages" % "1.79.2",
      "io.confluent" % "kafka-avro-serializer" % "5.0.1"
    )
  )
