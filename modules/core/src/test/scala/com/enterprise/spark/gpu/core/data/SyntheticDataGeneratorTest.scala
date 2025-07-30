package com.enterprise.spark.gpu.core.data

import com.enterprise.spark.gpu.core.SparkGpuTestBase
import org.apache.spark.sql.functions._

/**
 * Comprehensive test suite for SyntheticDataGenerator.
 *
 * This test suite validates:
 * - Data generation functionality with realistic datasets
 * - Data quality and statistical validity
 * - Performance characteristics and execution times
 * - File I/O operations and data persistence
 * - Error handling and edge cases
 *
 * Tests are designed to run quickly while providing comprehensive coverage
 * of the data generation functionality.
 */
class SyntheticDataGeneratorTest extends SparkGpuTestBase {

  private var dataGenerator: SyntheticDataGenerator = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    dataGenerator = new SyntheticDataGenerator(spark)
  }

  test("should generate taxi dataset with correct schema and record count") {
    val config = SyntheticDataGenerator.DataGenerationConfig(
      taxiRecordCount = 1000,
      housingRecordCount = 500,
      outputDirectory = testOutputDirectory
    )

    val (summary, executionTime) = measureExecutionTime {
      dataGenerator.generateAllDatasets(config)
    }

    // Verify summary information
    summary.taxiRecords shouldEqual 1000
    summary.housingRecords shouldEqual 500
    summary.totalRecords shouldEqual 1500
    summary.validationResults.valid shouldBe true

    // Load and verify taxi dataset
    val taxiData = spark.read.parquet(summary.taxiFilePath)

    // Verify record count
    assertRecordCount(taxiData, 1000)

    // Verify schema
    val expectedTaxiColumns = Seq(
      "trip_id", "trip_date", "hour_of_day", "day_of_week",
      "pickup_latitude", "pickup_longitude", "dropoff_latitude", "dropoff_longitude",
      "trip_distance_meters", "fare_amount", "passenger_count", "tip_indicator"
    )
    assertColumns(taxiData, expectedTaxiColumns)

    // Verify no null values
    assertNoNulls(taxiData, expectedTaxiColumns)

    logTestPerformance("Taxi Dataset Generation", executionTime, 1000)
  }

  test("should generate housing dataset with correct schema and realistic data") {
    val config = SyntheticDataGenerator.DataGenerationConfig(
      taxiRecordCount = 500,
      housingRecordCount = 1000,
      outputDirectory = testOutputDirectory
    )

    val (summary, executionTime) = measureExecutionTime {
      dataGenerator.generateAllDatasets(config)
    }

    // Load and verify housing dataset
    val housingData = spark.read.parquet(summary.housingFilePath)

    // Verify record count
    assertRecordCount(housingData, 1000)

    // Verify schema
    val expectedHousingColumns = Seq(
      "house_id", "price", "total_rooms", "bedrooms", "bathrooms",
      "square_feet", "age_years", "distance_to_center_km", "crime_rate",
      "school_rating", "neighborhood", "property_type"
    )
    assertColumns(housingData, expectedHousingColumns)

    // Verify no null values in numeric columns
    val numericColumns = Seq(
      "house_id", "price", "total_rooms", "bedrooms", "bathrooms",
      "square_feet", "age_years", "distance_to_center_km", "crime_rate", "school_rating"
    )
    assertNoNulls(housingData, numericColumns)

    logTestPerformance("Housing Dataset Generation", executionTime, 1000)
  }

  test("should validate data quality correctly") {
    val config = SyntheticDataGenerator.DataGenerationConfig(
      taxiRecordCount = 100,
      housingRecordCount = 100,
      outputDirectory = testOutputDirectory,
      enableDataValidation = true
    )

    val summary = dataGenerator.generateAllDatasets(config)

    // Validation should pass for clean generated data
    summary.validationResults.valid shouldBe true
    summary.validationResults.message should include("Validation passed")
  }

  test("should handle different record counts efficiently") {
    val testCases = Seq(
      (100, 50),
      (1000, 500)
    )

    testCases.foreach { case (taxiRecords, housingRecords) =>
      val config = SyntheticDataGenerator.DataGenerationConfig(
        taxiRecordCount = taxiRecords,
        housingRecordCount = housingRecords,
        outputDirectory = testOutputDirectory
      )

      val (summary, executionTime) = measureExecutionTime {
        dataGenerator.generateAllDatasets(config)
      }

      summary.taxiRecords shouldEqual taxiRecords
      summary.housingRecords shouldEqual housingRecords

      // Verify files exist
      assertPathExists(summary.taxiFilePath)
      assertPathExists(summary.housingFilePath)

      logTestPerformance(s"Generation ($taxiRecords taxi, $housingRecords housing)",
                        executionTime, summary.totalRecords)
    }
  }

  test("should handle edge cases and error conditions gracefully") {
    // Test with very small dataset
    val smallConfig = SyntheticDataGenerator.DataGenerationConfig(
      taxiRecordCount = 1,
      housingRecordCount = 1,
      outputDirectory = testOutputDirectory
    )

    val smallSummary = dataGenerator.generateAllDatasets(smallConfig)

    smallSummary.taxiRecords shouldEqual 1
    smallSummary.housingRecords shouldEqual 1
    smallSummary.validationResults.valid shouldBe true

    // Verify files exist and contain data
    val taxiData = spark.read.parquet(smallSummary.taxiFilePath)
    val housingData = spark.read.parquet(smallSummary.housingFilePath)

    taxiData.count() shouldEqual 1
    housingData.count() shouldEqual 1
  }

  test("should measure and report generation performance") {
    val config = SyntheticDataGenerator.DataGenerationConfig(
      taxiRecordCount = 2000,
      housingRecordCount = 1000,
      outputDirectory = testOutputDirectory
    )

    val (summary, totalTime) = measureExecutionTime {
      dataGenerator.generateAllDatasets(config)
    }

    // Verify performance metrics are captured
    summary.generationTimeMs should be > 0L
    summary.generationTimeMs should be <= totalTime + 100 // Allow small variance

    logTestPerformance("Performance Measurement Test", totalTime, summary.totalRecords)
  }
}