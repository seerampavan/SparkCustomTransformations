name := "SparkCustomTransformations"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

resolvers ++= Seq(
  "Apache Repository" at "https://repository.apache.org/content/repositories/releases/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-repl" % sparkVersion,
  "org.scala-lang" % "scala-compiler" % "2.11.8",
  "com.twitter" %% "util-eval" % "6.43.0"
)
