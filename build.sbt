lazy val deduplication = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .configs(IntegrationTest)
  .settings(inConfig(IntegrationTest)(Defaults.testSettings))
  .settings(
    organization := "com.ovoenergy.comms",
    organizationHomepage := Some(url("http://www.ovoenergy.com")),
    licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
    scalaVersion := "2.13.1",
    crossScalaVersions += "2.12.10",
    scalafmtOnCompile := true,
    scalacOptions -= "-Xfatal-warnings", // enable all options from sbt-tpolecat except fatal warnings
    initialCommands := s"import com.ovoenergy.comms.deduplication._",
    javacOptions ++= Seq("-Xlint:unchecked", "-Xlint:deprecation"),
    addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.11.0" cross CrossVersion.full),
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/ovotech/comms-deduplication"),
        "git@github.com:ovotech/comms-deduplication.git"
      )
    ),
    developers := List(
      Developer(
        "filosganga",
        "Filippo De Luca",
        "filippo.deluca@ovoenergy.com",
        url("https://github.com/filosganga")
      ),
      Developer(
        "SystemFw",
        "Fabio Labella",
        "fabio.labella@ovoenergy.com",
        url("https://github.com/SystemFw")
      )
    ),
    resolvers ++= Seq(
      Resolver.bintrayRepo("ovotech", "maven")
    ),
    excludeDependencies ++= Seq(
      ExclusionRule("commons-logging", "commons-logging")
    ),
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
      dep("org.typelevel")("2.1.2")(
        "cats-core"
      ) ++
        dep("org.typelevel")("2.1.1")(
          "cats-effect"
        ) ++
        dep("org.scala-lang.modules")("0.9.1")(
          "scala-java8-compat"
        ) ++
        dep("org.scanamo")("1.0.0-M11")(
          "scanamo",
          "scanamo-cats-effect"
        ) ++
        udep("org.slf4j")("1.7.30")(
          "slf4j-api"
        ) ++
        udep("org.slf4j")("1.7.30")(
          "jcl-over-slf4j"
        ).map(_ % IntegrationTest) ++
        Seq(
          "org.scalatest" %% "scalatest" % "3.1.1",
          "org.scalacheck" %% "scalacheck" % "1.14.3",
          "org.scalatestplus" %% "scalacheck-1-14" % "3.1.1.1",
          "ch.qos.logback" % "logback-classic" % "1.2.3"
        ).map(_ % IntegrationTest)
  )

def dep(org: String)(version: String)(libs: String*) = libs.map(org %% _ % version)
def udep(org: String)(version: String)(libs: String*) = libs.map(org % _ % version)
