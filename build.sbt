import sbtrelease.ExtraReleaseCommands
import sbtrelease.ReleaseStateTransformations._
import sbtrelease.tagsonly.TagsOnly._

val catsVersion = "2.12.0"

val catsEffectVersion = "3.5.7"

val slf4jVersion = "2.0.16"

val scalaJava8CompatVersion = "1.0.2"

val awsSdkVersion = "2.29.23"

val meteorVersion = "1.0.84"

val log4CatsVersion = "2.7.0"

val munitVersion = "0.7.29"

val logBackVersion = "1.5.12"

lazy val publicArtifactory = "Artifactory Realm" at "https://kaluza.jfrog.io/artifactory/maven"

lazy val publishSettings = Seq(
  publishTo := Some(publicArtifactory),
  credentials += {
    for {
      usr <- sys.env.get("ARTIFACTORY_USER")
      password <- sys.env.get("ARTIFACTORY_PASS")
    } yield Credentials("Artifactory Realm", "kaluza.jfrog.io", usr, password)
  }.getOrElse(Credentials(Path.userHome / ".ivy2" / ".credentials")),
  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    releaseStepCommand(ExtraReleaseCommands.initialVcsChecksCommand),
    setVersionFromTags(releaseTagPrefix.value),
    runClean,
    tagRelease,
    publishArtifacts,
    pushTagsOnly
  )
)

lazy val deduplication = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .configs(IntegrationTest)
  .settings(inConfig(IntegrationTest)(Defaults.testSettings))
  .settings(publishSettings)
  .settings(
    organization := "com.ovoenergy.comms",
    organizationHomepage := Some(url("http://www.ovoenergy.com")),
    licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
    scalaVersion := "2.13.9",
    crossScalaVersions += "2.12.18",
    scalafmtOnCompile := true,
    scalacOptions -= "-Xfatal-warnings", // enable all options from sbt-tpolecat except fatal warnings
    initialCommands := s"import com.ovoenergy.comms.deduplication._",
    javacOptions ++= Seq("-Xlint:unchecked", "-Xlint:deprecation"),
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision,
    testFrameworks += new TestFramework("munit.Framework"),
    addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.13.3" cross CrossVersion.full),
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
    excludeDependencies ++= Seq(
      ExclusionRule("commons-logging", "commons-logging")
    ),
    name := "deduplication",
    buildInfoPackage := "com.ovoenergy.comms.deduplication",
    version ~= (_.replace('+', '-')),
    dynver ~= (_.replace('+', '-')),
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-core" % catsVersion,
      "org.typelevel" %% "cats-effect" % catsEffectVersion,
      "org.scala-lang.modules" %% "scala-java8-compat" % scalaJava8CompatVersion,
      "software.amazon.awssdk" % "dynamodb" % awsSdkVersion,
      "io.github.d2a4u" %% "meteor-awssdk" % meteorVersion,
      "org.typelevel" %% "log4cats-core" % log4CatsVersion,
      "org.typelevel" %% "log4cats-slf4j" % log4CatsVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.typelevel" %% "cats-effect-laws" % catsEffectVersion % Test,
      "org.slf4j" % "jcl-over-slf4j" % slf4jVersion % IntegrationTest,
      "org.scalameta" %% "munit" % munitVersion % s"${Test};${IntegrationTest}",
      "org.scalameta" %% "munit-scalacheck" % munitVersion % s"${Test};${IntegrationTest}",
      "ch.qos.logback" % "logback-classic" % logBackVersion % s"${Test};${IntegrationTest}"
    )
  )
