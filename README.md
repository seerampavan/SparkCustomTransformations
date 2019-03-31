# Read and execute string scala spark code. 

## Seperate the UDF code execution from actual spark execution

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

    // UDF code and UDF name
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

## Output

    +---+---------+------------+-------------+-------------+-------------+-------------+
    |Key|fakePatID|fakeEventTag|customUDFCol1|customUDFCol2|customUDFCol3|customUDFCol4|
    +---+---------+------------+-------------+-------------+-------------+-------------+
    |  1|       21|       ES-80|        es-80|        ES-80|            1|           -1|
    |  2|       21|       ES-80|        es-80|        ES-80|            1|           -1|
    |  3|       21|       ES-80|        es-80|        ES-80|            1|           -1|
    |  4|       21|       ES-81|        es-81|        ES-81|            1|           -1|
    |  5|       21|       ES-81|        es-81|        ES-81|            1|           -1|
    |  6|       21|       ES-81|        es-81|        ES-81|            1|           -1|
    |  7|       21|       ES-82|        es-82|        ES-82|            1|           -1|
    |  8|       21|       ES-82|        es-82|        ES-82|            1|           -1|
    |  9|       21|       ES-82|        es-82|        ES-82|            1|           -1|
    | 10|       22|       PS-83|        ps-83|        PS-83|            2|           -1|
    | 11|       22|       PS-83|        ps-83|        PS-83|            2|           -1|
    | 12|       22|       PS-83|        ps-83|        PS-83|            2|           -1|
    | 13|       22|       PS-83|        ps-83|        PS-83|            2|           -1|
    | 14|       22|       PS-84|        ps-84|        PS-84|            2|           -1|
    | 15|       22|       PS-84|        ps-84|        PS-84|            2|           -1|
    | 16|       22|       PS-84|        ps-84|        PS-84|            2|           -1|
    | 17|       22|       PS-84|        ps-84|        PS-84|            2|           -1|
    | 18|       23|       WS-85|        ws-85|        WS-85|            2|           -1|
    | 19|       23|       WS-85|        ws-85|        WS-85|            2|           -1|
    | 20|       23|       WS-85|        ws-85|        WS-85|            2|           -1|
    +---+---------+------------+-------------+-------------+-------------+-------------+
    only showing top 20 rows
    
    root
     |-- Key: string (nullable = true)
     |-- fakePatID: string (nullable = true)
     |-- fakeEventTag: string (nullable = true)
     |-- customUDFCol1: string (nullable = true)
     |-- customUDFCol2: string (nullable = true)
     |-- customUDFCol3: integer (nullable = true)
     |-- customUDFCol4: integer (nullable = true)
    
    +---+---------+------------+-------------+-------------+-------------+-------------+
    |Key|fakePatID|fakeEventTag|customUDFCol1|customUDFCol2|customUDFCol3|customUDFCol4|
    +---+---------+------------+-------------+-------------+-------------+-------------+
    |  1|       21|       ES-80|        es-80|        ES-80|            1|           -1|
    |  2|       21|       ES-80|        es-80|        ES-80|            1|           -1|
    |  3|       21|       ES-80|        es-80|        ES-80|            1|           -1|
    |  4|       21|       ES-81|        es-81|        ES-81|            1|           -1|
    |  5|       21|       ES-81|        es-81|        ES-81|            1|           -1|
    |  6|       21|       ES-81|        es-81|        ES-81|            1|           -1|
    |  7|       21|       ES-82|        es-82|        ES-82|            1|           -1|
    |  8|       21|       ES-82|        es-82|        ES-82|            1|           -1|
    |  9|       21|       ES-82|        es-82|        ES-82|            1|           -1|
    | 10|       22|       PS-83|        ps-83|        PS-83|            2|           -1|
    | 11|       22|       PS-83|        ps-83|        PS-83|            2|           -1|
    | 12|       22|       PS-83|        ps-83|        PS-83|            2|           -1|
    | 13|       22|       PS-83|        ps-83|        PS-83|            2|           -1|
    | 14|       22|       PS-84|        ps-84|        PS-84|            2|           -1|
    | 15|       22|       PS-84|        ps-84|        PS-84|            2|           -1|
    | 16|       22|       PS-84|        ps-84|        PS-84|            2|           -1|
    | 17|       22|       PS-84|        ps-84|        PS-84|            2|           -1|
    | 18|       23|       WS-85|        ws-85|        WS-85|            2|           -1|
    | 19|       23|       WS-85|        ws-85|        WS-85|            2|           -1|
    | 20|       23|       WS-85|        ws-85|        WS-85|            2|           -1|
    +---+---------+------------+-------------+-------------+-------------+-------------+
    only showing top 20 rows