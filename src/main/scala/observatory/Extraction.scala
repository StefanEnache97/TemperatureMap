package observatory



import org.apache.spark.sql._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DateType, DoubleType, FloatType, IntegerType, StringType, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import java.time.LocalDate
import scala.io.Source
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col


/**
  * 1st milestone: data extraction
  */
object Extraction extends ExtractionInterface {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val conf: SparkConf = new SparkConf()
    .setAppName("OP Observatory")
    .setMaster("local[2]")

  val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  sparkSession.sparkContext.setLogLevel("WARN")

  import sparkSession.implicits._


  val stationsSchema: StructType =
    new StructType()
      .add("STN_station", IntegerType, nullable = true)
      .add("WBAN_station", IntegerType, nullable = true)
      .add("Lat", DoubleType, nullable = true)
      .add("Long", DoubleType, nullable = true)

  val temperaturesSchema: StructType =
    new StructType()
      .add("STN_temp", IntegerType, nullable = true)
      .add("WBAN_temp", IntegerType, nullable = true)
      .add("Month", IntegerType, nullable = true)
      .add("Day", IntegerType, nullable = true)
      .add("Temperature", DoubleType, nullable = true)

  def getDSFromResource(resource: String): Dataset[String] = {
    val fileStream = Source.getClass.getResourceAsStream(resource)

    sparkSession.sparkContext.makeRDD(Source.fromInputStream(fileStream).getLines().toList).toDS
  }


  def locateTemperatures_RDD(year: Year, stationsFile: String, temperaturesFile: String): DataFrame = {
    //Extract records from stationFile and temperatureFile into DataFrames using respective schemas
    val rawTemperatureRecords = sparkSession.read.schema(temperaturesSchema).csv(getDSFromResource(temperaturesFile))
    val rawStationRecords = sparkSession.read.schema(stationsSchema).csv(getDSFromResource(stationsFile))

    val cleanStationRecords = rawStationRecords.filter(r => !(((r.isNullAt(2) || r.getAs[Double](2) == 0.0) || (r.isNullAt(3) || r.getAs[Double](3) == 0.0)) || (r.isNullAt(1) && r.isNullAt(0))))
      .na.fill(-1,Seq("STN_station", "WBAN_station"))
    val cleanTemperatureRecords = rawTemperatureRecords.filter(r => !((r.isNullAt(0) && r.isNullAt(1)) || r.getAs[Double](4) == 9999.9))
      .withColumn("Temperature", (col("Temperature") - 32.0)*(5.0/9.0))
      .na.fill(-1, Seq("STN_temp", "WBAN_temp"))

    val joinedRecords = cleanTemperatureRecords
      .join(cleanStationRecords,
        cleanStationRecords("STN_station") === cleanTemperatureRecords("STN_temp") && cleanStationRecords("WBAN_station") === cleanTemperatureRecords("WBAN_temp"))
      .select($"Lat", $"Long", $"Month", $"Day", $"Temperature")

    joinedRecords


  }

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {

    locateTemperatures_RDD(year, stationsFile, temperaturesFile)
      .map(r => ((year, r.getAs[Int](2), r.getAs[Int](3)), Location.apply(r.getAs[Double](0), r.getAs[Double](1)), r.getAs[Double](4)))
      .collect().toSeq
      .map(e => (LocalDate.of(e._1._1, e._1._2, e._1._3), e._2, e._3 ))

  }


  def locationYearlyAverageRecords_RDD(records: RDD[(LocalDate, Location, Temperature)]): RDD[(Location, Temperature)] = {
    records
      .groupBy(r => (r._1.getYear, r._2))
      .aggregateByKey((0: Int,0.0: Double))((agg, r) => (agg._1 + r.size, agg._2 + r.map(e => e._3).sum), (r1, r2) => (r1._1 + r2._1, r1._2 + r2._2))
      .map(r => (r._1._2, r._2._2/r._2._1))

  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {

    val records_RDD = sparkSession.sparkContext.parallelize(records.toSeq)
    val yearlyAverageSeq = locationYearlyAverageRecords_RDD(records_RDD).collect().toSeq

    yearlyAverageSeq
  }


}
