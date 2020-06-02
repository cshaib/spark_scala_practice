import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

object exerciseOne extends App {
    // start spark session -- local means it will run it on one machine (not distributed)
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Spark Exercise")
      .getOrCreate()

    // set logging level
    spark.sparkContext.setLogLevel("ERROR")

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    // read in example pipe-separated file into Dataframe
    val rawDf = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", "|")
      .load("/Users/chantal/Desktop/rand.csv")

    // add column names
    val colNames = Seq("consumerID", "partyID", "ABID")
    val df = rawDf.toDF(colNames:_*)

    // preview first 5 rows
    // df.show(5)

    // Method one: using DF to agg
    val aggDfWay = df.groupBy("consumerID")
                     .agg(
                         collect_list("partyID") as "partyIDs",
                         collect_list("ABID") as "ABIDs")
                     .sort("consumerID")

    println("Grouping by consumerID using Dataframe API")
    aggDfWay.show()

    // Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("data")

    // Method two: using SQL query to agg
    val aggSQLWay = spark.sql(
        """
          |SELECT consumerID, COLLECT_LIST(partyID),  COLLECT_LIST(ABID)
          |FROM data
          |GROUP BY consumerID
          |ORDER BY consumerID
          |""".stripMargin
    )

    println("Grouping by consumerID directly using SQL")
    aggSQLWay.show()
}
