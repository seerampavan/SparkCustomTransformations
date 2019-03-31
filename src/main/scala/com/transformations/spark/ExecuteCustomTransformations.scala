package com.transformations.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ExecuteCustomTransformations {

  def main(args: Array[String]): Unit = {
    setLogLevels(Level.WARN, Seq("spark", "org", "akka"))

    lazy val spark = getSparkSession

    val udf_str1 =
      """
        (str:String)=>{
          str.toLowerCase
        }
        """
    val udf_str2 =
      """
        (str:String)=>{
          str.capitalize
        }
        """

    val udf_str3 =
      """
        (str:String)=>{
          str.length
        }
        """

    val udf_str4 =
      """
        (str:String)=>{
          Option(str.indexOf(1))
        }
        """

    val udf_str5 =
      """
        (str: Option[String])=>{
          str.map(_.toLowerCase)
        }
        """
    val customTransformationsStr =
      """
        |      df.createOrReplaceTempView("df_view")
        |      val df_updated = spark.sql("select a.*, udf1(a.fakeEventTag) as customUDFCol1, udf2(a.fakeEventTag) as customUDFCol2 from df_view a").withColumn("customUDFCol3", udf3_udf(col("Key"))).withColumn("customUDFCol4", udf4_udf($"fakeEventTag"))
        |      df_updated.show()
        |      df_updated
      """.stripMargin

    val udfList: List[(String, String)] = List((udf_str1, "udf1"),
                                               (udf_str2, "udf2"),
                                               (udf_str3, "udf3"),
                                               (udf_str4, "udf4"),
                                               (udf_str5, "udf5"))

    val customTransformations = CustomTransformationsEval.getInstance()

    val sampleSparkDF = getSampleDataframe(spark)

    val (customTransObj, udfObjectList) = customTransformations
      .getCustomTransformationWithUDFs(udfList, customTransformationsStr)

    val df = customTransObj.execute(spark, sampleSparkDF, udfObjectList: _*)

    df.printSchema()
    df.show()

  }

  def getSampleDataframe(spark: SparkSession) = {
    var summaryDF = spark.read
      .option("header", true)
      .option("multiLine", true)
      .option("escape", "\"")
      .csv("data/data.csv")
      .drop("eventTitleNM", "blobcontentsTXT")

    summaryDF
  }

  def setLogLevels(level: Level, loggers: Seq[String]): Map[String, Level] =
    loggers
      .map(loggerName => {
        val logger = Logger.getLogger(loggerName)
        val prevLevel = logger.getLevel
        logger.setLevel(level)
        loggerName -> prevLevel
      })
      .toMap

  def getSparkSession = {
    val sparkConfig = new SparkConf()
    SparkSession.builder
      .master("local[*]")
      .appName("Test1")
      .config(sparkConfig)
      .getOrCreate()
  }
}
