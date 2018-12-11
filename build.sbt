import sbt.Keys.version
import sbt.Resolver

lazy val Versions = new {
  val hbase = "1.2.0-cdh5.13.1"
  val spark = "2.3.0.cloudera4"
  val scala = "2.11.8"
}

resolvers += "ClouderaRepo" at "https://repository.cloudera.com/artifactory/cloudera-repos"
resolvers += "Local Maven Repository" at "file:///home/paul/.m2/repository"
resolvers += Resolver.bintrayRepo("ons", "ONS-Registers")

lazy val root = (project in file("."))

  .enablePlugins(BuildInfoPlugin)

  .settings(
    resourceDirectory in Compile := baseDirectory.value / "resources",
    resourceDirectory in Test := baseDirectory.value / "resources",

    scalaVersion := Versions.scala,
    name := "sbr-assembler-calculations",
    version := "0.1",
    mainClass in(Compile, run) := Some("AssemblerCalculations"),

    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion,
      BuildInfoKey.action("buildTime") {
        System.currentTimeMillis
      }),
    buildInfoPackage := "pipeline",

    libraryDependencies ++= Seq(

      "uk.gov.ons" % "registers-sml" % "1.12",
      "uk.gov.ons" % "sbr-assembler-common" % "0.1-SNAPSHOT",
      "commons-cli" % "commons-cli" % "1.4",
      "com.typesafe" % "config" % "1.3.2",

      "org.scalatest" %% "scalatest" % "2.2.6" % "test",
      "org.apache.hbase" % "hbase-hadoop-compat" % Versions.hbase,

      ("org.apache.hbase" % "hbase-server" % Versions.hbase)
        .exclude("com.sun.jersey", "jersey-server")
        .exclude("org.mortbay.jetty", "jsp-api-2.1"),

      "org.apache.hbase" % "hbase-common" % Versions.hbase,
      "org.apache.hbase" % "hbase-client" % Versions.hbase,
      ("org.apache.spark" %% "spark-core" % Versions.spark)
        .exclude("aopalliance", "aopalliance")
        .exclude("commons-beanutils", "commons-beanutils"),

      "org.apache.spark" %% "spark-sql" % Versions.spark,
      ("org.apache.crunch" % "crunch-hbase" % "0.15.0")
        .exclude("com.sun.jersey", "jersey-server")

    )
  )
