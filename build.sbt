import sbtassembly.Plugin.AssemblyKeys._

name := "CSV2Parquet"

version := "1.0"

scalaVersion := "2.11.4"

autoScalaLibrary := true

assemblySettings

test in assembly := {}

//assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = true)

mergeStrategy in assembly <<= (mergeStrategy in assembly) {
  (old) => {
    case PathList("META-INF", xs@_ *) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}

//mainClass in assembly := Some("com.bds.etl.user.HiveFromSpar")

jarName in assembly := "etlUsers.jar"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.0" % "provided"

libraryDependencies += "org.postgresql" % "postgresql" % "9.3-1102-jdbc41"

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "1.8.0"