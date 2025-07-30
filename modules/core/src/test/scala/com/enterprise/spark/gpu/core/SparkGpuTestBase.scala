package com.enterprise.spark.gpu.core

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import java.io.File
import java.nio.file.{Files, Paths}

/**
 * Base test class providing common Spark testing infrastructure.
 *
 * This class provides:
 * - Shared Spark session management
 * - Test data cleanup utilities
 * - Common test assertions and matchers
 * - Performance measurement utilities
 *
 * All test classes should extend this base class to ensure consistent
 * test environment and proper resource management.
 */
abstract class SparkGpuTestBase extends AnyFunSuite
  with Matchers
  with BeforeAndAfterAll
  with BeforeAndAfterEach {

  protected var spark: SparkSession = _
  protected val testDataDirectory = "test-data"
  protected val testOutputDirectory = "test-output"

  /**
   * Initialize Spark session before all tests in the suite.
   */
  override def beforeAll(): Unit = {
    super.beforeAll()
    println("Initializing test Spark session")

    // Create test-optimized Spark session (CPU-only for consistent test results)
    spark = SparkSession.builder()
      .appName("SparkGpuTestSuite")
      .master("local[2]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", "4") // Small for tests
      .config("spark.executor.memory", "1g")
      .config("spark.driver.memory", "1g")
      .getOrCreate()

    // Set log level to reduce test output noise
    spark.sparkContext.setLogLevel("WARN")

    // Create test directories
    createTestDirectories()

    println("Test Spark session initialized successfully")
  }

  /**
   * Clean up Spark session after all tests complete.
   */
  override def afterAll(): Unit = {
    try {
      if (spark != null) {
        println("Stopping test Spark session")
        spark.stop()
      }
      // Clean up test directories
      cleanupTestDirectories()
    } finally {
      super.afterAll()
    }
  }

  /**
   * Clean up test data before each test.
   */
  override def beforeEach(): Unit = {
    super.beforeEach()
    cleanupTestOutput()
  }

  /**
   * Creates necessary test directories.
   */
  private def createTestDirectories(): Unit = {
    val directories = Seq(testDataDirectory, testOutputDirectory, "test-models", "test-reports")
    directories.foreach { dir =>
      val path = Paths.get(dir)
      if (!Files.exists(path)) {
        Files.createDirectories(path)
        println(s"Created test directory: $dir")
      }
    }
  }

  /**
   * Cleans up test output directory.
   */
  private def cleanupTestOutput(): Unit = {
    val outputDir = new File(testOutputDirectory)
    if (outputDir.exists()) {
      deleteDirectory(outputDir)
      outputDir.mkdirs()
    }
  }

  /**
   * Cleans up all test directories.
   */
  private def cleanupTestDirectories(): Unit = {
    val directories = Seq(testDataDirectory, testOutputDirectory, "test-models", "test-reports")
    directories.foreach { dir =>
      val directory = new File(dir)
      if (directory.exists()) {
        deleteDirectory(directory)
        println(s"Cleaned up test directory: $dir")
      }
    }
  }

  /**
   * Recursively deletes a directory and all its contents.
   */
  private def deleteDirectory(directory: File): Unit = {
    if (directory.exists()) {
      Option(directory.listFiles()).foreach { files =>
        files.foreach { file =>
          if (file.isDirectory) {
            deleteDirectory(file)
          } else {
            file.delete()
          }
        }
      }
      directory.delete()
    }
  }

  /**
   * Measures execution time of a code block.
   *
   * @param block Code block to measure
   * @return Tuple of (result, execution time in milliseconds)
   */
  protected def measureExecutionTime[T](block: => T): (T, Long) = {
    val startTime = System.currentTimeMillis()
    val result = block
    val executionTime = System.currentTimeMillis() - startTime
    (result, executionTime)
  }

  /**
   * Asserts that a DataFrame has the expected number of records.
   */
  protected def assertRecordCount(df: org.apache.spark.sql.DataFrame, expectedCount: Long): Unit = {
    val actualCount = df.count()
    actualCount shouldEqual expectedCount
  }

  /**
   * Asserts that a DataFrame has the expected columns.
   */
  protected def assertColumns(df: org.apache.spark.sql.DataFrame, expectedColumns: Seq[String]): Unit = {
    val actualColumns = df.columns.toSeq.sorted
    val sortedExpected = expectedColumns.sorted
    actualColumns shouldEqual sortedExpected
  }

  /**
   * Asserts that a DataFrame has no null values in specified columns.
   */
  protected def assertNoNulls(df: org.apache.spark.sql.DataFrame, columns: Seq[String]): Unit = {
    columns.foreach { column =>
      val nullCount = df.filter(df(column).isNull).count()
      nullCount shouldEqual 0
    }
  }

  /**
   * Asserts that execution time is within acceptable bounds.
   */
  protected def assertExecutionTime(actualTimeMs: Long, maxExpectedTimeMs: Long): Unit = {
    actualTimeMs should be <= maxExpectedTimeMs
  }

  /**
   * Asserts that a file or directory exists.
   */
  protected def assertPathExists(path: String): Unit = {
    val file = new File(path)
    file.exists() shouldBe true
  }

  /**
   * Asserts that model metrics are within acceptable ranges.
   */
  protected def assertModelMetrics(
    rmse: Double,
    mae: Double,
    r2: Double,
    maxRmse: Double = Double.MaxValue,
    maxMae: Double = Double.MaxValue,
    minR2: Double = 0.0
  ): Unit = {
    rmse should be <= maxRmse
    mae should be <= maxMae
    r2 should be >= minR2

    // Basic sanity checks
    rmse should be >= 0.0
    mae should be >= 0.0
    r2 should be <= 1.0
  }

  /**
   * Creates sample test data for quick tests.
   */
  protected def createSampleHousingData(recordCount: Int = 100): org.apache.spark.sql.DataFrame = {
    import spark.implicits._

    val data = (1 to recordCount).map { id =>
      (
        id, // house_id
        100000.0 + (id * 1000), // price
        3 + (id % 4), // total_rooms
        2 + (id % 3), // bedrooms
        1 + (id % 2), // bathrooms
        1000 + (id * 10), // square_feet
        id % 50, // age_years
        id % 20, // distance_to_center_km
        id % 10, // crime_rate
        (id % 10) + 1, // school_rating
        if (id % 3 == 0) "Downtown" else "Suburbs", // neighborhood
        if (id % 2 == 0) "Single Family" else "Condo" // property_type
      )
    }

    data.toDF(
      "house_id", "price", "total_rooms", "bedrooms", "bathrooms",
      "square_feet", "age_years", "distance_to_center_km", "crime_rate",
      "school_rating", "neighborhood", "property_type"
    )
  }

  /**
   * Logs test performance metrics.
   */
  protected def logTestPerformance(testName: String, executionTimeMs: Long, recordsProcessed: Long = 0): Unit = {
    println(s"Test Performance - $testName:")
    println(s"  Execution Time: ${executionTimeMs}ms")
    if (recordsProcessed > 0) {
      val recordsPerSecond = (recordsProcessed * 1000.0 / executionTimeMs).toLong
      println(s"  Records Processed: $recordsProcessed")
      println(s"  Throughput: $recordsPerSecond records/second")
    }
  }
}