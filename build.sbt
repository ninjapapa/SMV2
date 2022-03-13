name := "smv"

organization := "org.tresamigos"

version := "2-SNAPSHOT"

scalaVersion := "2.12.10"

scalacOptions ++= Seq("-deprecation", "-feature")

// Compile against earliest supported Spark - the jar will be foward compatible.
val sparkVersion = "2.4.8"

libraryDependencies ++= Seq(
  "org.apache.spark"             %% "spark-sql"         % sparkVersion % "provided"
)

parallelExecution in Test := false

publishArtifact in Test := true

testOptions in Test += Tests.Argument("-oF")

mainClass in assembly := Some("org.tresamigos.smv.SmvApp")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyJarName in assembly := s"${name.value}-${version.value}-jar-with-dependencies.jar"

test in assembly := {}

// initialize ~= { _ => sys.props("scalac.patmat.analysisBudget") = "off" }
