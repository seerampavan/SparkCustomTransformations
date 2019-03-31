package com.transformations.spark

import java.io._
import java.util.concurrent.atomic.AtomicReference
import com.twitter.util.Eval
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

  // read the string class object of com.transformations.spark.CustomTransformations and convert to actual object
  private val eval = new Eval
  private def decodeCustomTransform(
      transformationCode: String): CustomTransformations =
    eval[CustomTransformations](transformationCode)

  private val intp = getInterpreter

  /**
    * parse the UDF code and return UDF function and UDF name
    * @param udfCode raw UDF code
    * @return UDF function and UDF its function class type (ex: "Function2[String, String, String]")
    */
  private def decodeUDF(udfCode: String): (AnyRef, String) =
    ExceptionHandler {
      // interpreting and compiling the raw UDF code
      intp.interpret(udfCode)
      val udfFunction = intp.eval(udfCode)

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

    val cl = ClassLoader.getSystemClassLoader
    val settings = new GenericRunnerSettings(println _)

    if (sparkConfOpt.nonEmpty || additionalJarsRefPaths.nonEmpty) {
      val jars = (additionalJarsRefPaths ++ getSparkUserJars(sparkConfOpt) ++ cl
        .asInstanceOf[java.net.URLClassLoader]
        .getURLs
        .map(_.toString)).mkString(File.pathSeparator)
      val interpArguments = List(
        "-classpath",
        jars
      )
      settings.processArguments(interpArguments, true)
    }
    settings.usejavacp.value = true

    val intp = new IMain(settings, out)
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
      //returning com.transformations.spark.CustomTransformations object along with UDF's list in AnyRef type
      (decodeCustomTransform(customTransClassStr), udfDetails.map(_._2))
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
