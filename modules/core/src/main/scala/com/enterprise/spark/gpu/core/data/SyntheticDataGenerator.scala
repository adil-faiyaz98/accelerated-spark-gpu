package com.enterprise.spark.gpu.core.data

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io.File
import scala.util.Random

/**
 * Synthetic data generator for GPU acceleration benchmarking.
 *
 * This service generates realistic datasets that demonstrate clear performance
 * differences between CPU and GPU execution. The generated data includes:
 * - Taxi trip data with temporal patterns and fare calculations
 * - Housing market data with multiple features for ML model training
 * - Large-scale datasets that benefit from GPU parallel processing
 *
 * All generated datasets are designed to showcase GPU acceleration benefits
 * in real-world scenarios while maintaining data quality and statistical validity.
 */
class SyntheticDataGenerator(spark: SparkSession) extends LazyLogging {

  import spark.implicits._

  /**
   * Configuration for data generation with realistic scale and complexity.
   */
  case class DataGenerationConfig(
    taxiRecordCount: Int = 1000000, // 1M records for meaningful GPU benefits
    housingRecordCount: Int = 500000, // 500K records for ML training
    outputDirectory: String = "data",
    enableDataValidation: Boolean = true,
    compressionEnabled: Boolean = true
  )

  /**
   * Generates all synthetic datasets required for the GPU acceleration demonstration.
   *
   * @param config Configuration parameters for data generation
   * @return Summary of generated datasets with record counts and file sizes
   */
  def generateAllDatasets(config: DataGenerationConfig = DataGenerationConfig()): DataGenerationSummary = {
    logger.info("Starting comprehensive synthetic data generation")
    logger.info(s"Target directory: ${config.outputDirectory}")
    logger.info(s"Taxi records: ${config.taxiRecordCount:,}")
    logger.info(s"Housing records: ${config.housingRecordCount:,}")

    // Ensure output directory exists
    createOutputDirectory(config.outputDirectory)
    val startTime = System.currentTimeMillis()

    // Generate taxi dataset
    logger.info("Generating taxi trip dataset...")
    val taxiDataset = generateTaxiDataset(config.taxiRecordCount)
    val taxiPath = saveDataset(taxiDataset, s"${config.outputDirectory}/taxi_trips", config.compressionEnabled)

    // Generate housing dataset
    logger.info("Generating housing market dataset...")
    val housingDataset = generateHousingDataset(config.housingRecordCount)
    val housingPath = saveDataset(housingDataset, s"${config.outputDirectory}/housing_market", config.compressionEnabled)

    // Validate generated data if requested
    val validationResults = if (config.enableDataValidation) {
      logger.info("Validating generated datasets...")
      validateGeneratedData(taxiDataset, housingDataset)
    } else {
      DataValidationResults(valid = true, "Validation skipped")
    }

    val totalTime = System.currentTimeMillis() - startTime
    val summary = DataGenerationSummary(
      taxiRecords = config.taxiRecordCount,
      housingRecords = config.housingRecordCount,
      taxiFilePath = taxiPath,
      housingFilePath = housingPath,
      generationTimeMs = totalTime,
      validationResults = validationResults
    )

    logger.info(s"Data generation completed in ${totalTime}ms")
    logger.info(s"Generated ${summary.totalRecords:,} total records")
    summary
  }

  /**
   * Generates realistic taxi trip data with temporal patterns and fare calculations.
   */
  private def generateTaxiDataset(recordCount: Int): DataFrame = {
    logger.info(s"Creating taxi dataset with $recordCount records")

    // Generate base trip data
    val baseData = (1 to recordCount).map { tripId =>
      val random = new Random(tripId) // Deterministic for reproducibility

      // Generate realistic temporal patterns
      val dayOfYear = random.nextInt(365) + 1
      val hourOfDay = generateRealisticHour(random)
      val dayOfWeek = (dayOfYear % 7) + 1

      // Generate pickup and dropoff coordinates (Manhattan-like grid)
      val pickupLat = 40.7128 + (random.nextGaussian() * 0.1)
      val pickupLon = -74.0060 + (random.nextGaussian() * 0.1)
      val dropoffLat = pickupLat + (random.nextGaussian() * 0.05)
      val dropoffLon = pickupLon + (random.nextGaussian() * 0.05)

      // Calculate trip distance (simplified Euclidean for demo)
      val distance = math.sqrt(
        math.pow(dropoffLat - pickupLat, 2) + math.pow(dropoffLon - pickupLon, 2)
      ) * 111000 // Convert to meters approximately

      // Generate realistic fare based on distance, time, and demand
      val baseFare = 2.50
      val perMeterRate = 0.002
      val timeMultiplier = if (hourOfDay >= 7 && hourOfDay <= 9 || hourOfDay >= 17 && hourOfDay <= 19) 1.5 else 1.0
      val fareAmount = baseFare + (distance * perMeterRate * timeMultiplier) + random.nextGaussian() * 2.0

      (
        tripId, // trip_id
        s"2023-${(dayOfYear / 30) + 1}-${dayOfYear % 30 + 1}", // trip_date
        hourOfDay, // hour_of_day
        dayOfWeek, // day_of_week
        pickupLat, // pickup_latitude
        pickupLon, // pickup_longitude
        dropoffLat, // dropoff_latitude
        dropoffLon, // dropoff_longitude
        distance, // trip_distance_meters
        math.max(1.0, fareAmount), // fare_amount
        random.nextInt(5) + 1, // passenger_count
        if (random.nextDouble() < 0.15) "Y" else "N" // tip_indicator
      )
    }

    // Convert to DataFrame with proper schema
    val schema = StructType(Array(
      StructField("trip_id", IntegerType, nullable = false),
      StructField("trip_date", StringType, nullable = false),
      StructField("hour_of_day", IntegerType, nullable = false),
      StructField("day_of_week", IntegerType, nullable = false),
      StructField("pickup_latitude", DoubleType, nullable = false),
      StructField("pickup_longitude", DoubleType, nullable = false),
      StructField("dropoff_latitude", DoubleType, nullable = false),
      StructField("dropoff_longitude", DoubleType, nullable = false),
      StructField("trip_distance_meters", DoubleType, nullable = false),
      StructField("fare_amount", DoubleType, nullable = false),
      StructField("passenger_count", IntegerType, nullable = false),
      StructField("tip_indicator", StringType, nullable = false)
    ))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(baseData).map(row => org.apache.spark.sql.Row(
        row._1, row._2, row._3, row._4, row._5, row._6, row._7, row._8, row._9, row._10, row._11, row._12
      )),
      schema
    )

    logger.info(s"Generated taxi dataset with ${df.count()} records")
    df
  }

  /**
   * Generates realistic housing market data for ML model training.
   */
  private def generateHousingDataset(recordCount: Int): DataFrame = {
    logger.info(s"Creating housing dataset with $recordCount records")

    val baseData = (1 to recordCount).map { houseId =>
      val random = new Random(houseId)

      // Generate correlated housing features
      val rooms = random.nextInt(6) + 2 // 2-7 rooms
      val bedrooms = math.min(rooms - 1, random.nextInt(4) + 1)
      val bathrooms = math.min(rooms, random.nextInt(3) + 1)
      val squareFeet = 500 + (rooms * 200) + random.nextInt(1000)
      val age = random.nextInt(100) // 0-99 years old
      val distanceToCenter = random.nextDouble() * 50 // 0-50 km
      val crimeRate = random.nextDouble() * 10 // 0-10 scale
      val schoolRating = random.nextInt(10) + 1 // 1-10 scale

      // Generate realistic price based on features
      val basePrice = 100000
      val roomMultiplier = rooms * 15000
      val sizeMultiplier = squareFeet * 100
      val ageDiscount = age * 500
      val locationMultiplier = math.max(0.5, 2.0 - (distanceToCenter / 25))
      val crimeDiscount = crimeRate * 2000
      val schoolBonus = schoolRating * 3000
      val price = (basePrice + roomMultiplier + sizeMultiplier - ageDiscount + schoolBonus - crimeDiscount) * locationMultiplier + (random.nextGaussian() * 20000)

      val neighborhood = Array("Downtown", "Suburbs", "Waterfront", "Historic", "Industrial")(random.nextInt(5))
      val propertyType = Array("Single Family", "Condo", "Townhouse", "Duplex")(random.nextInt(4))

      (
        houseId, // house_id
        math.max(50000, price), // price (target variable)
        rooms, // total_rooms
        bedrooms, // bedrooms
        bathrooms, // bathrooms
        squareFeet, // square_feet
        age, // age_years
        distanceToCenter, // distance_to_center_km
        crimeRate, // crime_rate
        schoolRating, // school_rating
        neighborhood, // neighborhood
        propertyType // property_type
      )
    }

    baseData.toDF(
      "house_id", "price", "total_rooms", "bedrooms", "bathrooms",
      "square_feet", "age_years", "distance_to_center_km", "crime_rate",
      "school_rating", "neighborhood", "property_type"
    )
  }

  /**
   * Generates realistic hour distribution with peak patterns.
   */
  private def generateRealisticHour(random: Random): Int = {
    // Create realistic hourly distribution with morning and evening peaks
    val hourWeights = Array(
      0.5, 0.3, 0.2, 0.2, 0.3, 0.8, 1.5, 2.5, 2.0, 1.2, // 0-9
      1.0, 1.1, 1.3, 1.2, 1.1, 1.0, 1.2, 2.0, 2.5, 1.8, // 10-19
      1.5, 1.2, 0.8, 0.6 // 20-23
    )

    val totalWeight = hourWeights.sum
    val randomValue = random.nextDouble() * totalWeight
    var cumulativeWeight = 0.0

    for (hour <- hourWeights.indices) {
      cumulativeWeight += hourWeights(hour)
      if (randomValue <= cumulativeWeight) {
        return hour
      }
    }
    23 // Fallback
  }

  /**
   * Creates output directory if it doesn't exist.
   */
  private def createOutputDirectory(path: String): Unit = {
    val dir = new File(path)
    if (!dir.exists()) {
      dir.mkdirs()
      logger.info(s"Created output directory: $path")
    }
  }

  /**
   * Saves dataset to disk with optional compression and persistence.
   */
  private def saveDataset(df: DataFrame, path: String, compress: Boolean): String = {
    val writer = df.coalesce(1).write.mode("overwrite")
    if (compress) {
      writer.option("compression", "snappy")
    }
    writer.parquet(path)
    logger.info(s"Saved dataset to: $path")

    // Also save a sample as CSV for repository transparency
    val samplePath = path.replace("data/", "sample-data/sample_") + ".csv"
    val sampleSize = math.min(1000, df.count().toInt)
    try {
      df.limit(sampleSize)
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(samplePath)
      logger.info(s"Saved sample dataset to: $samplePath")
    } catch {
      case e: Exception =>
        logger.warn(s"Could not save sample dataset: ${e.getMessage}")
    }

    path
  }

  /**
   * Validates the quality and consistency of generated data.
   */
  private def validateGeneratedData(taxiData: DataFrame, housingData: DataFrame): DataValidationResults = {
    try {
      // Basic validation checks
      val taxiCount = taxiData.count()
      val housingCount = housingData.count()

      // Check for null values
      val taxiNulls = taxiData.columns.map(col => taxiData.filter(taxiData(col).isNull).count()).sum
      val housingNulls = housingData.columns.map(col => housingData.filter(housingData(col).isNull).count()).sum

      val isValid = taxiCount > 0 && housingCount > 0 && taxiNulls == 0 && housingNulls == 0

      val message = if (isValid) {
        s"Validation passed: Taxi records: $taxiCount, Housing records: $housingCount, No null values"
      } else {
        s"Validation failed: Taxi nulls: $taxiNulls, Housing nulls: $housingNulls"
      }

      DataValidationResults(isValid, message)
    } catch {
      case e: Exception =>
        logger.error("Data validation failed", e)
        DataValidationResults(valid = false, s"Validation error: ${e.getMessage}")
    }
  }
}

/**
 * Summary of data generation results.
 */
case class DataGenerationSummary(
  taxiRecords: Int,
  housingRecords: Int,
  taxiFilePath: String,
  housingFilePath: String,
  generationTimeMs: Long,
  validationResults: DataValidationResults
) {
  def totalRecords: Int = taxiRecords + housingRecords
  def generationTimeSeconds: Double = generationTimeMs / 1000.0
}

/**
 * Results of data validation checks.
 */
case class DataValidationResults(
  valid: Boolean,
  message: String
)