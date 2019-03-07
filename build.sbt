lazy val deduplication = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    name := "deduplication",
    organization := "com.ovoenergy.comms",
    organizationHomepage := Some(url("http://www.ovoenergy.com")),
    licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
    scalaVersion := "2.12.8",

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

    scalafmtOnCompile := true,

    buildInfoPackage := "deduplication",
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
      dep("com.ovoenergy")("1.8.11")(
        "comms-docker-testkit-core",
        "comms-docker-testkit-clients"
      ).map(_ % Test)
  )

def dep(org: String)(version: String)(libs: String*) = libs.map(org %% _ % version)