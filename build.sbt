
lazy val IT = config("it") extend Test

lazy val deduplication = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .configs(IT)
  .settings(inConfig(IT)(Defaults.testSettings))
  .settings(
    inThisBuild(List(
      organization := "com.ovoenergy.comms",
      organizationHomepage := Some(url("http://www.ovoenergy.com")),
      licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
      scalaVersion := "2.12.10",

      scmInfo := Some(ScmInfo(
        url("https://github.com/ovotech/comms-deduplication"),
        "git@github.com:ovotech/comms-deduplication.git"
      )),

      developers := List(
        Developer(
          "filosganga",
          "Filippo De Luca",
          "filippo.deluca@ovoenergy.com",
          url("https://github.com/filosganga")
        ),
        Developer(
          "ZsoltBalvanyos",
          "Zsolt Balvanyos",
          "zsolt.balvanyos@ovoenergy.com",
          url("https://github.com/ZsoltBalvanyos")
        ),
        Developer(
          "SystemFw",
          "Fabio Labella",
          "fabio.labella@ovoenergy.com",
          url("https://github.com/SystemFw")
        )
      ),

      resolvers ++= Seq(
        Resolver.bintrayRepo("ovotech", "maven"),
      ),

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

      excludeDependencies ++= Seq(
        ExclusionRule("commons-logging", "commons-logging")
      ),

      scalafmtOnCompile := true,
    ))
  )
  .settings(
    name := "deduplication",

    buildInfoPackage := "com.ovoenergy.comms.deduplication",
    version ~= (_.replace('+', '-')),
    dynver ~= (_.replace('+', '-')),

    bintrayOrganization := Some("ovotech"),
    bintrayPackage := { "comms-" ++ moduleName.value },
    bintrayPackageLabels := Seq("duplication", "deduplication"),
    bintrayRepository := "maven",
    bintrayOmitLicense := true,

    releaseEarlyWith := BintrayPublisher,
    releaseEarlyEnableSyncToMaven := false,
    releaseEarlyNoGpg := true,

    libraryDependencies ++=
      dep("org.typelevel")("1.6.1")(
        "cats-core"
      ) ++
      dep("org.typelevel")("1.4.0")(
        "cats-effect"
      ) ++
      dep("org.scala-lang.modules")("0.9.0")(
        "scala-java8-compat"
      ) ++
      dep("com.gu")("1.0.0-M8")(
        "scanamo",
        "scanamo-formats"
      ) ++
      udep("org.slf4j")("1.7.29")(
        "slf4j-api"
      ) ++
      udep("org.slf4j")("1.7.29")(
        "jcl-over-slf4j"
      ).map(_ % Test) ++
      Seq(
        "org.scalatest" %% "scalatest" % "3.0.8",
        "org.scalacheck" %% "scalacheck" % "1.14.3",
        "ch.qos.logback" % "logback-classic" % "1.2.3",
      ).map(_ % Test) ++
      dep("com.ovoenergy")("1.8.11")(
        "comms-docker-testkit-core",
        "comms-docker-testkit-clients"
      ).map(_ % Test)
  )

def dep(org: String)(version: String)(libs: String*) = libs.map(org %% _ % version)
def udep(org: String)(version: String)(libs: String*) = libs.map(org % _ % version)