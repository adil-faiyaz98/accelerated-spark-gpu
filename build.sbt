ThisBuild / organization := "com.enterprise.spark.gpu"
ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.12.17" // Common settings for all modules
lazy val commonSettings = Seq( scalacOptions ++= Seq( "-deprecation", "-feature", "-unchecked", "-Xlint", "-Ywarn-dead-code", "-Ywarn-numeric-widen", "-Ywarn-value-discard" ), javacOptions ++= Seq("-source", "11", "-target", "11"), Test / parallelExecution := false, Test / fork := true, Test / javaOptions ++= Seq("-Xmx4g", "-XX:+UseG1GC")
) // Dependency versions
val sparkVersion = "3.5.1"
val xgboostVersion = "1.7.3"
val rapidsVersion = "24.02.0"
val scalatestVersion = "3.2.15"
val scalaLoggingVersion = "3.9.5"
val logbackVersion = "1.4.11"
val allureVersion = "2.24.0" // Common dependencies
lazy val commonDependencies = Seq( "org.apache.spark" %% "spark-core" % sparkVersion, "org.apache.spark" %% "spark-sql" % sparkVersion, "org.apache.spark" %% "spark-mllib" % sparkVersion, "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion, "ch.qos.logback" % "logback-classic" % logbackVersion, "org.scalatest" %% "scalatest" % scalatestVersion % Test, "io.qameta.allure" % "allure-scalatest" % allureVersion % Test
) lazy val gpuDependencies = Seq( "ml.dmlc" %% "xgboost4j-spark" % xgboostVersion, "com.nvidia" %% "rapids-4-spark" % rapidsVersion
) // Root project
lazy val root = (project in file(".")) .aggregate(core, benchmarking, reporting, integration) .settings( name := "accelerated-spark-gpu", commonSettings, publish / skip := true ) // Core module - main pipeline logic
lazy val core = (project in file("modules/core")) .settings( name := "spark-gpu-core", commonSettings, libraryDependencies ++= commonDependencies ++ gpuDependencies ) // Benchmarking module - performance testing
lazy val benchmarking = (project in file("modules/benchmarking")) .dependsOn(core) .settings( name := "spark-gpu-benchmarking", commonSettings, libraryDependencies ++= commonDependencies ++ Seq( "org.scalactic" %% "scalactic" % scalatestVersion, "com.github.tototoshi" %% "scala-csv" % "1.3.10" ) ) // Reporting module - report generation and visualization
lazy val reporting = (project in file("modules/reporting")) .dependsOn(core, benchmarking) .settings( name := "spark-gpu-reporting", commonSettings, libraryDependencies ++= commonDependencies ++ Seq( "org.jfree" % "jfreechart" % "1.5.3", "com.github.tototoshi" %% "scala-csv" % "1.3.10", "org.apache.commons" % "commons-math3" % "3.6.1" ) ) // Integration module - end-to-end tests
lazy val integration = (project in file("modules/integration")) .dependsOn(core, benchmarking, reporting) .settings( name := "spark-gpu-integration", commonSettings, libraryDependencies ++= commonDependencies ++ gpuDependencies ++ Seq( "org.testcontainers" % "testcontainers" % "1.19.1" % Test, "org.testcontainers" % "junit-jupiter" % "1.19.1" % Test ) ) // Global resolvers
ThisBuild / resolvers ++= Seq( "NVIDIA Maven" at "https://repo1.maven.org/maven2/", "Apache Snapshots" at "https://repository.apache.org/snapshots/", "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
) // Test reporting configuration
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-h", "target/test-reports/html")
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-u", "target/test-reports/junit") // Assembly configuration for fat JAR
assembly / assemblyMergeStrategy := { case PathList("META-INF", xs @ _*) => MergeStrategy.discard case "application.conf" => MergeStrategy.concat case "reference.conf" => MergeStrategy.concat case _ => MergeStrategy.first
}