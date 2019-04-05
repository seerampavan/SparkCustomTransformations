package com.transformations.spark

import java.io._
import java.nio.file._
import java.net.URL
import java.util.concurrent.atomic.AtomicReference
import java.util.jar.JarInputStream
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import scala.tools.nsc._
import scala.tools.nsc.interpreter._
import scala.util._

object ExceptionHandler {
  def apply[T](f: => T): T =
    Try(f) match {
      case Success(result) => result
      case Failure(exception) =>
        println(exception.getStackTrace.mkString("\n", "\n", "\n"))
        throw exception
    }
}

trait CustomTransformations extends Serializable {
  def execute(spark: SparkSession,
              df: DataFrame,
              udfFunctions: AnyRef*): DataFrame
}

class CustomTransformationsEval(sparkConfOpt: Option[SparkConf] = None,
                                additionalJarsRefPaths: Seq[String] =
                                  Seq.empty[String]) {

  //TODO Need to handle the writer object in efficient way in future
  private var output = new StringWriter
  private val out: JPrintWriter = new java.io.PrintWriter(output)

  private val intp = getInterpreter

  private def decodeObject(interpretCode: String): AnyRef = {
    // interpreting and compiling the raw code
    intp.interpret(interpretCode)
    intp.eval(interpretCode)
  }

  /**
    * parse the UDF code and return UDF function and UDF name
    * @param udfCode raw UDF code
    * @return UDF function and UDF its function class type (ex: "Function2[String, String, String]")
    */
  private def decodeUDF(udfCode: String): (AnyRef, String) =
    ExceptionHandler {
      // interpreting and compiling the raw UDF code
      val udfFunction = decodeObject(udfCode)

      // capturing the last line from the StringWriter to get the above evaluated string function
      val outputStr = output.toString.substring(0, output.toString.length - 1)
      val unparsedFuncType =
        Try(outputStr.substring(outputStr.lastIndexOf("\n"))).toOption
          .map(_.replaceAll("\n", ""))
          .filter(_.nonEmpty)
          .fold(outputStr)(x => x)
      // parsing string UDF string definition ex: "res0: (String, String) => Int = <function2>" to actual function type ex: "Function2[String, String, String]" by using regex
      val udfTypeDetails = raw"[0-9a-zA-Z()\[\], ]+(?![^(]*\})".r
        .findAllIn(unparsedFuncType)
        .toList
        .map(_.trim.capitalize.replaceAll("\\(", "").replaceAll("\\)", ""))
        .filter(_.nonEmpty)
        .tail
      val udfFunctionType =
        if (udfTypeDetails.nonEmpty && udfTypeDetails.size == 3) {
          val functionType = udfTypeDetails.last
          val functionArgs = udfTypeDetails.slice(0, 2).mkString(", ")
          s"$functionType[$functionArgs]"
        } else
          throw new RuntimeException(
            s"Unable to render the custom UDF function \n $udfCode")
      // returning UDF function as AnyRef and its type ex: "Function2[String, String, String]"
      (udfFunction, udfFunctionType)
    }

  /**
    * read the optional spark and additional jars and load it to Interpreter
    * @return Interpreter
    */
  private def getInterpreter: IMain = ExceptionHandler {

    // read the spark configuration and add the required spark jars to the classloader
    def getSparkUserJars(confOpt: Option[SparkConf]): Seq[String] =
      confOpt.fold(Seq.empty[String]) { conf =>
        val sparkJars = conf.getOption("spark.jars")
        if (conf.get("spark.master") == "yarn") {
          val yarnJars = conf.getOption("spark.yarn.dist.jars")
          unionFileLists(sparkJars, yarnJars).toSeq
        } else {
          sparkJars.map(_.split(",")).map(_.filter(_.nonEmpty)).toSeq.flatten
        }
      }

    def unionFileLists(leftList: Option[String],
                       rightList: Option[String]): Set[String] = {
      var allFiles = Set[String]()
      leftList.foreach { value =>
        allFiles ++= value.split(",")
      }
      rightList.foreach { value =>
        allFiles ++= value.split(",")
      }
      allFiles.filter(_.nonEmpty)
    }

    val classPathJar = sys.props("java.class.path")
    val rootJarPath =
      classPathJar.substring(classPathJar.lastIndexOf(File.pathSeparator) + 1)

    val jarsClassPath =
      if (rootJarPath.nonEmpty && rootJarPath.contains("classpath")) {
        val rootJarLocation =
          rootJarPath.substring(0, rootJarPath.lastIndexOf("/") + 1)

        val jarFileStream = new java.io.FileInputStream(new File(rootJarPath))
        val jarStream = new JarInputStream(jarFileStream)
        val mf = jarStream.getManifest
        jarStream.getManifest.getMainAttributes
          .getValue(java.util.jar.Attributes.Name.CLASS_PATH)
          .split(" ")
          .mkString(rootJarLocation, File.pathSeparator + rootJarLocation, "")
      } else {
        def getClasspathUrls(classLoader: ClassLoader,
                             acc: List[URL]): List[URL] =
          classLoader match {
            case null => acc
            case cl: java.net.URLClassLoader =>
              getClasspathUrls(classLoader.getParent, acc ++ cl.getURLs.toList)
            case _ => getClasspathUrls(classLoader.getParent, acc)
          }

        val classpathUrls =
          getClasspathUrls(this.getClass.getClassLoader, List())
        val classpathElements = classpathUrls map (_.toURI.getPath)
        classpathElements.mkString(File.pathSeparator)
      }

    val finalJarsClassPath =
      if (sparkConfOpt.nonEmpty || additionalJarsRefPaths.nonEmpty) {
        jarsClassPath + ":" +
          (additionalJarsRefPaths ++ getSparkUserJars(sparkConfOpt))
            .filter { jarPath =>
              val fileExists = Files.exists(Paths.get(jarPath))
              if (!fileExists) println(s"""
                                    |Unable find the jar at the location $jarPath
                                    |So ignoring this jar file to put in classpath """.stripMargin)
              fileExists
            }
            .mkString(File.pathSeparator)
      } else jarsClassPath

    val settings = new Settings()

    settings.classpath append finalJarsClassPath

    val intp = new IMain(settings, out)
    println(intp.global.classPath.asClassPathString)
    intp.setContextClassLoader
    intp.initializeSynchronous

    intp
  }

  /**
    * Get com.transformations.spark.CustomTransformations object along with UDF's list
    * @param udfList
    * @param customTransformationsStr
    * @return com.transformations.spark.CustomTransformations object along with UDF's list in AnyRef type
    */
  def getCustomTransformationWithUDFs(
      udfList: List[(String, String)],
      customTransformationsStr: String): (CustomTransformations, List[AnyRef]) =
    ExceptionHandler {
      // read the udf list and create a string definition of it, to add it in com.transformations.spark.CustomTransformations class string object for interpreting
      val udfDetails = udfList.zipWithIndex.map {
        case ((udfCode, udfName), index) =>
          val (udfDefinition, udfType) = decodeUDF(udfCode)
          (udfName,
           udfDefinition,
           s"""
               |      // UDF definition for $udfName
               |      val ${udfName}_udf = spark.udf.register("${udfName}", udfFunctions($index).asInstanceOf[$udfType])
           """.stripMargin)
      }
      // preparing the code definition for UDF
      val codeForAllUDFs = udfDetails
        .map(_._3)
        .mkString("\n      /*********UDF code starts here********/\n",
                  "\n",
                  "\n      /*********UDF code ends here********/\n")
      // creating the string object notation of the com.transformations.spark.CustomTransformations by adding UDF's and user transformations
      val customTransClassStr =
        s"""
           |import org.apache.spark.SparkConf
           |import org.apache.spark.sql.{DataFrame, SparkSession}
           |import org.apache.spark.sql.functions._
           |
           |new com.transformations.spark.CustomTransformations {
           |    override def execute(spark: SparkSession, df: DataFrame, udfFunctions: AnyRef*): DataFrame = {
           |      import spark.implicits._
           |      $codeForAllUDFs
           |      $customTransformationsStr
           |    }
           |}
      """.stripMargin
      // read the string class object of com.transformations.spark.CustomTransformations and convert to actual object
      // returning com.transformations.spark.CustomTransformations object along with UDF's list in AnyRef type
      (decodeObject(customTransClassStr).asInstanceOf[CustomTransformations],
       udfDetails.map(_._2))
    }
}

object CustomTransformationsEval {
  // creating singleton object
  private val evalInstance = new AtomicReference[CustomTransformationsEval]()

  def getInstance(sparkConfOpt: Option[SparkConf] = None,
                  additionalJarsRefPaths: Seq[String] = Seq.empty[String])
    : CustomTransformationsEval = evalInstance.synchronized {
    if (Option(evalInstance.get()).isEmpty)
      evalInstance.set(
        new CustomTransformationsEval(sparkConfOpt, additionalJarsRefPaths))
    evalInstance.get()
  }

}
