lazy val deduplication = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    name := "deduplication",
    organization := "com.ovoenergy.comms",
    organizationHomepage := Some(url("http://www.ovoenergy.com")),
    scalaVersion := "2.12.8",

    scalafmtOnCompile := true,

    buildInfoPackage := "deduplication",
    version ~= (_.replace('+', '-')),
    dynver ~= (_.replace('+', '-')),

    bintrayOrganization := Some("ovotech"),
    bintrayRepository := "maven",
    bintrayOmitLicense := true,

    releaseEarlyWith := BintrayPublisher,
    releaseEarlyEnableSyncToMaven := false,
    releaseEarlyNoGpg := true,

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
      Resolver.bintrayRepo("ovotech", "maven"),
    ),

    libraryDependencies ++=
      dep("org.typelevel")("1.6.0")(
        "cats-core"
      ) ++
      dep("org.typelevel")("1.2.0")(
        "cats-effect"
      ) ++
      dep("com.gu")("1.0.0-M7")(
        "scanamo",
        "scanamo-formats"
      ) ++
      Seq(
        "org.scalatest" %% "scalatest" % "3.0.1",
        "org.scalacheck" %% "scalacheck" % "1.13.5",
      ).map(_ % Test) ++
      dep("com.ovoenergy")("1.8.9-3-16ed0aab-20190306-1042")(
        "comms-docker-testkit-core",
        "comms-docker-testkit-clients"
      ).map(_ % Test)
  )

def dep(org: String)(version: String)(libs: String*) = libs.map(org %% _ % version)