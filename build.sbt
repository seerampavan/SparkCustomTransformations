version := "1.0"

val sparkVersion = "2.2.0"

lazy val commonSettings = Seq(
  version := "1.0.0-SNAPSHOT",
  scalaVersion := "2.11.8"
)
lazy val sparkCustomTransformations = (project in file("."))
  .enablePlugins(ClasspathJarPlugin)
  .settings(commonSettings: _*)
  .settings(
    name := "SparkCustomTransformations",
    assemblyJarName := "SparkCustomTransformations.jar",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-hive" % sparkVersion,
      "org.apache.spark" %% "spark-mllib" % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "org.scala-lang" % "scala-compiler" % "2.11.8"
    ),
    scriptClasspath in bashScriptDefines ~= (cp => "/$CUSTOM_CLASSPATH" +: cp),
    retrieveManaged := false,
    mainClass in assembly := Some("com.transformations.spark.ExecuteCustomTransformations"),
    javaOptions in assembly += "-xmx6g",
    resolvers ++= Seq(
      "Apache Repository" at "https://repository.apache.org/content/repositories/releases/",
      "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
      Resolver.sonatypeRepo("public")
    )
  )