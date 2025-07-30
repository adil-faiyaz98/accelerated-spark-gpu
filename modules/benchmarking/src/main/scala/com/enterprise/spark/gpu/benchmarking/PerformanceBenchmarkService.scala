package com.enterprise.spark.gpu.benchmarking

import com.enterprise.spark.gpu.core.config.SparkGpuConfiguration
import com.enterprise.spark.gpu.core.data.SyntheticDataGenerator
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.ml.regression.RandomForestRegressor
import scala.collection.mutable.ListBuffer
import java.io.File

/**
 * Performance benchmarking service for GPU vs CPU comparison.
 *
 * This service provides comprehensive benchmarking capabilities including:
 * - ETL operation performance comparison (CPU vs GPU)
 * - Machine learning training speed analysis
 * - Data aggregation and transformation benchmarks
 * - Memory usage and resource utilization metrics
 * - Statistical analysis of performance improvements
 *
 * The benchmarking is designed to provide measurable, reproducible evidence
 * of GPU acceleration benefits in real-world Spark workloads.
 */
class PerformanceBenchmarkService extends LazyLogging {

  private val configuration = new SparkGpuConfiguration()

  /**
   * Configuration for benchmark execution.
   */
  case class BenchmarkConfig(
    iterations: Int = 3,
    warmupIterations: Int = 1,
    datasetSizes: Seq[Int] = Seq(100000, 500000, 1000000),
    enableGpuComparison: Boolean = true,
    outputDirectory: String = "reports/benchmarks"
  )

  /**
   * Executes comprehensive performance benchmarks comparing CPU and GPU execution.
   *
   * This method runs a complete suite of benchmarks including:
   * - ETL operations (filtering, aggregations, joins)
   * - Machine learning model training
   * - Complex data transformations
   * - Memory and resource utilization analysis
   *
   * @param config Benchmark configuration parameters
   * @return Comprehensive benchmark results with performance metrics
   */
  def executeComprehensiveBenchmarks(config: BenchmarkConfig = BenchmarkConfig()): BenchmarkResults = {
    logger.info("=== Starting Comprehensive Performance Benchmarks ===")
    logger.info(s"Iterations: ${config.iterations}, Warmup: ${config.warmupIterations}")
    logger.info(s"Dataset sizes: ${config.datasetSizes.mkString(", ")}")
    logger.info(s"GPU comparison enabled: ${config.enableGpuComparison}")

    val startTime = System.currentTimeMillis()
    val benchmarkResults = ListBuffer[BenchmarkResult]()

    try {
      // Create output directory
      createOutputDirectory(config.outputDirectory)

      // Run benchmarks for each dataset size
      config.datasetSizes.foreach { datasetSize =>
        logger.info(s"Running benchmarks for dataset size: $datasetSize")

        // Generate test data
        val testData = generateBenchmarkData(datasetSize)

        // CPU Benchmarks
        logger.info("Executing CPU benchmarks...")
        val cpuResults = executeCpuBenchmarks(testData, config)
        benchmarkResults ++= cpuResults

        // GPU Benchmarks (if enabled and available)
        if (config.enableGpuComparison && configuration.isGpuAvailable()) {
          logger.info("Executing GPU benchmarks...")
          val gpuResults = executeGpuBenchmarks(testData, config)
          benchmarkResults ++= gpuResults
        } else {
          logger.info("GPU benchmarks skipped - GPU not available or disabled")
        }
      }

      val totalExecutionTime = System.currentTimeMillis() - startTime
      val results = BenchmarkResults(
        benchmarkResults = benchmarkResults.toSeq,
        totalExecutionTimeMs = totalExecutionTime,
        gpuAvailable = configuration.isGpuAvailable(),
        config = config
      )

      logger.info(s"=== Benchmarks Completed in ${totalExecutionTime}ms ===")
      logBenchmarkSummary(results)
      results

    } catch {
      case e: Exception =>
        logger.error("Benchmark execution failed", e)
        throw new RuntimeException(s"Performance benchmarking failed: ${e.getMessage}", e)
    }
  } /** * Generates synthetic data optimized for benchmarking different operations. */ private def generateBenchmarkData(recordCount: Int): BenchmarkDataset = { logger.info(s"Generating benchmark dataset with $recordCount records") // Create CPU-optimized Spark session for data generation val spark = SparkSession.builder() .appName("BenchmarkDataGeneration") .master("local[*]") .config("spark.sql.adaptive.enabled", "true") .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") .getOrCreate() try { val dataGenerator = new SyntheticDataGenerator(spark) val config = SyntheticDataGenerator.DataGenerationConfig( taxiRecordCount = recordCount, housingRecordCount = recordCount / 2, outputDirectory = "benchmark-data", enableDataValidation = false, // Skip validation for speed compressionEnabled = false // Disable compression for faster I/O ) val generationSummary = dataGenerator.generateAllDatasets(config) // Load datasets into memory for benchmarking val taxiData = spark.read.parquet(generationSummary.taxiFilePath).cache() val housingData = spark.read.parquet(generationSummary.housingFilePath).cache() // Force caching by triggering actions taxiData.count() housingData.count() BenchmarkDataset( taxiData = taxiData, housingData = housingData, recordCount = recordCount, spark = spark ) } catch { case e: Exception => spark.stop() throw e } } /** * Executes CPU-optimized benchmarks. */ private def executeCpuBenchmarks(dataset: BenchmarkDataset, config: BenchmarkConfig): Seq[BenchmarkResult] = { val results = ListBuffer[BenchmarkResult]() // Create CPU-optimized Spark session val cpuSpark = configuration.createOptimizedSparkSession( appName = "CPU-Benchmark", enableGpu = false ) try { // Re-load data in CPU session val taxiData = cpuSpark.read.parquet("benchmark-data/taxi_trips").cache() val housingData = cpuSpark.read.parquet("benchmark-data/housing_market").cache() // ETL Benchmarks results += benchmarkEtlOperations(taxiData, housingData, cpuSpark, "CPU", config) // ML Training Benchmarks results += benchmarkMlTraining(housingData, cpuSpark, "CPU", config) // Aggregation Benchmarks results += benchmarkAggregations(taxiData, cpuSpark, "CPU", config) } finally { cpuSpark.stop() } results.toSeq } /** * Executes GPU-optimized benchmarks. */ private def executeGpuBenchmarks(dataset: BenchmarkDataset, config: BenchmarkConfig): Seq[BenchmarkResult] = { val results = ListBuffer[BenchmarkResult]() // Create GPU-optimized Spark session val gpuSpark = configuration.createOptimizedSparkSession( appName = "GPU-Benchmark", enableGpu = true ) try { // Re-load data in GPU session val taxiData = gpuSpark.read.parquet("benchmark-data/taxi_trips").cache() val housingData = gpuSpark.read.parquet("benchmark-data/housing_market").cache() // ETL Benchmarks results += benchmarkEtlOperations(taxiData, housingData, gpuSpark, "GPU", config) // ML Training Benchmarks results += benchmarkMlTraining(housingData, gpuSpark, "GPU", config) // Aggregation Benchmarks results += benchmarkAggregations(taxiData, gpuSpark, "GPU", config) } finally { gpuSpark.stop() } results.toSeq } /** * Benchmarks ETL operations including filtering, joins, and transformations. */ private def benchmarkEtlOperations( taxiData: DataFrame, housingData: DataFrame, spark: SparkSession, executionMode: String, config: BenchmarkConfig ): BenchmarkResult = { logger.info(s"Benchmarking ETL operations in $executionMode mode") val operationTimes = ListBuffer[Long]() // Warmup iterations (1 to config.warmupIterations).foreach { _ => performEtlOperations(taxiData, housingData, spark) } // Actual benchmark iterations (1 to config.iterations).foreach { iteration => logger.debug(s"ETL benchmark iteration $iteration/$config.iterations") val startTime = System.currentTimeMillis() performEtlOperations(taxiData, housingData, spark) val executionTime = System.currentTimeMillis() - startTime operationTimes += executionTime } val avgTime = operationTimes.sum / operationTimes.length val minTime = operationTimes.min val maxTime = operationTimes.max BenchmarkResult( operationType = "ETL_Operations", executionMode = executionMode, datasetSize = taxiData.count() + housingData.count(), averageTimeMs = avgTime, minTimeMs = minTime, maxTimeMs = maxTime, iterations = config.iterations, throughputRecordsPerSecond = ((taxiData.count() + housingData.count()) * 1000.0 / avgTime).toLong ) } /** * Performs actual ETL operations for benchmarking. */ private def performEtlOperations(taxiData: DataFrame, housingData: DataFrame, spark: SparkSession): Unit = { // Complex filtering and aggregation val filteredTaxi = taxiData .filter(col("fare_amount") > 5.0) .filter(col("trip_distance_meters") > 1000) .groupBy("hour_of_day") .agg( avg("fare_amount").as("avg_fare"), sum("trip_distance_meters").as("total_distance"), count("*").as("trip_count") ) // Force execution filteredTaxi.count() // Complex housing data transformations val processedHousing = housingData .withColumn("price_per_sqft", col("price") / col("square_feet")) .withColumn("room_efficiency", col("square_feet") / col("total_rooms")) .filter(col("price") > 100000) .groupBy("neighborhood") .agg( avg("price").as("avg_price"), avg("price_per_sqft").as("avg_price_per_sqft"), count("*").as("property_count") ) // Force execution processedHousing.count() } /** * Benchmarks machine learning model training. */ private def benchmarkMlTraining( housingData: DataFrame, spark: SparkSession, executionMode: String, config: BenchmarkConfig ): BenchmarkResult = { logger.info(s"Benchmarking ML training in $executionMode mode") val trainingTimes = ListBuffer[Long]() // Prepare feature pipeline val featureColumns = Array("total_rooms", "bedrooms", "bathrooms", "square_feet", "age_years", "distance_to_center_km", "crime_rate", "school_rating") val assembler = new VectorAssembler() .setInputCols(featureColumns) .setOutputCol("raw_features") val scaler = new StandardScaler() .setInputCol("raw_features") .setOutputCol("features") .setWithStd(true) .setWithMean(true) val randomForest = new RandomForestRegressor() .setLabelCol("price") .setFeaturesCol("features") .setNumTrees(50) // Reduced for benchmarking .setMaxDepth(8) .setSeed(42) val pipeline = new Pipeline().setStages(Array(assembler, scaler, randomForest)) // Warmup iterations (1 to config.warmupIterations).foreach { _ => val model = pipeline.fit(housingData) // Force model creation model.transform(housingData.limit(10)).count() } // Actual benchmark iterations (1 to config.iterations).foreach { iteration => logger.debug(s"ML training benchmark iteration $iteration/${config.iterations}") val startTime = System.currentTimeMillis() val model = pipeline.fit(housingData) // Force model creation and prediction model.transform(housingData.limit(100)).count() val executionTime = System.currentTimeMillis() - startTime trainingTimes += executionTime } val avgTime = trainingTimes.sum / trainingTimes.length val minTime = trainingTimes.min val maxTime = trainingTimes.max BenchmarkResult( operationType = "ML_Training", executionMode = executionMode, datasetSize = housingData.count(), averageTimeMs = avgTime, minTimeMs = minTime, maxTimeMs = maxTime, iterations = config.iterations, throughputRecordsPerSecond = (housingData.count() * 1000.0 / avgTime).toLong ) } /** * Benchmarks complex aggregation operations. */ private def benchmarkAggregations( taxiData: DataFrame, spark: SparkSession, executionMode: String, config: BenchmarkConfig ): BenchmarkResult = { logger.info(s"Benchmarking aggregations in $executionMode mode") val aggregationTimes = ListBuffer[Long]() // Warmup iterations (1 to config.warmupIterations).foreach { _ => performComplexAggregations(taxiData) } // Actual benchmark iterations (1 to config.iterations).foreach { iteration => logger.debug(s"Aggregation benchmark iteration $iteration/${config.iterations}") val startTime = System.currentTimeMillis() performComplexAggregations(taxiData) val executionTime = System.currentTimeMillis() - startTime aggregationTimes += executionTime } val avgTime = aggregationTimes.sum / aggregationTimes.length val minTime = aggregationTimes.min val maxTime = aggregationTimes.max BenchmarkResult( operationType = "Complex_Aggregations", executionMode = executionMode, datasetSize = taxiData.count(), averageTimeMs = avgTime, minTimeMs = minTime, maxTimeMs = maxTime, iterations = config.iterations, throughputRecordsPerSecond = (taxiData.count() * 1000.0 / avgTime).toLong ) } /** * Performs complex aggregation operations for benchmarking. */ private def performComplexAggregations(taxiData: DataFrame): Unit = { // Multi-dimensional aggregations val hourlyStats = taxiData .groupBy("hour_of_day", "day_of_week") .agg( avg("fare_amount").as("avg_fare"), stddev("fare_amount").as("stddev_fare"), min("fare_amount").as("min_fare"), max("fare_amount").as("max_fare"), sum("trip_distance_meters").as("total_distance"), count("*").as("trip_count") ) // Force execution hourlyStats.count() // Percentile calculations val farePercentiles = taxiData .select( expr("percentile_approx(fare_amount, 0.25)").as("fare_25th"), expr("percentile_approx(fare_amount, 0.5)").as("fare_median"), expr("percentile_approx(fare_amount, 0.75)").as("fare_75th"), expr("percentile_approx(fare_amount, 0.95)").as("fare_95th") ) // Force execution farePercentiles.collect() } /** * Creates output directory for benchmark results. */ private def createOutputDirectory(path: String): Unit = { val dir = new File(path) if (!dir.exists()) { dir.mkdirs() logger.info(s"Created benchmark output directory: $path") } } /** * Logs comprehensive benchmark summary. */ private def logBenchmarkSummary(results: BenchmarkResults): Unit = { logger.info("=== Benchmark Results Summary ===") logger.info(s"Total execution time: ${results.totalExecutionTimeMs}ms") logger.info(s"GPU available: ${results.gpuAvailable}") logger.info(s"Total benchmark operations: ${results.benchmarkResults.length}") // Group results by operation type and execution mode val groupedResults = results.benchmarkResults.groupBy(_.operationType) groupedResults.foreach { case (operationType, operationResults) => logger.info(s"\n$operationType Results:") val cpuResults = operationResults.filter(_.executionMode == "CPU") val gpuResults = operationResults.filter(_.executionMode == "GPU") if (cpuResults.nonEmpty) { val avgCpuTime = cpuResults.map(_.averageTimeMs).sum / cpuResults.length logger.info(s" CPU Average Time: ${avgCpuTime}ms") } if (gpuResults.nonEmpty) { val avgGpuTime = gpuResults.map(_.averageTimeMs).sum / gpuResults.length logger.info(s" GPU Average Time: ${avgGpuTime}ms") if (cpuResults.nonEmpty) { val avgCpuTime = cpuResults.map(_.averageTimeMs).sum / cpuResults.length val speedup = avgCpuTime.toDouble / avgGpuTime logger.info(s" GPU Speedup: ${speedup.formatted("%.2f")}x") } } } logger.info("=== Benchmark Summary Complete ===") }

  /**
   * Generates synthetic data optimized for benchmarking different operations.
   */
  private def generateBenchmarkData(recordCount: Int): BenchmarkDataset = {
    logger.info(s"Generating benchmark dataset with $recordCount records")

    // Create CPU-optimized Spark session for data generation
    val spark = SparkSession.builder()
      .appName("BenchmarkDataGeneration")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    try {
      val dataGenerator = new SyntheticDataGenerator(spark)
      val config = SyntheticDataGenerator.DataGenerationConfig(
        taxiRecordCount = recordCount,
        housingRecordCount = recordCount / 2,
        outputDirectory = "benchmark-data",
        enableDataValidation = false, // Skip validation for speed
        compressionEnabled = false // Disable compression for faster I/O
      )

      val generationSummary = dataGenerator.generateAllDatasets(config)

      // Load datasets into memory for benchmarking
      val taxiData = spark.read.parquet(generationSummary.taxiFilePath).cache()
      val housingData = spark.read.parquet(generationSummary.housingFilePath).cache()

      // Force caching by triggering actions
      taxiData.count()
      housingData.count()

      BenchmarkDataset(
        taxiData = taxiData,
        housingData = housingData,
        recordCount = recordCount,
        spark = spark
      )
    } catch {
      case e: Exception =>
        spark.stop()
        throw e
    }
  }

  /**
   * Executes CPU-optimized benchmarks.
   */
  private def executeCpuBenchmarks(dataset: BenchmarkDataset, config: BenchmarkConfig): Seq[BenchmarkResult] = {
    val results = ListBuffer[BenchmarkResult]()

    // Create CPU-optimized Spark session
    val cpuSpark = configuration.createOptimizedSparkSession(
      appName = "CPU-Benchmark",
      enableGpu = false
    )

    try {
      // Re-load data in CPU session
      val taxiData = cpuSpark.read.parquet("benchmark-data/taxi_trips").cache()
      val housingData = cpuSpark.read.parquet("benchmark-data/housing_market").cache()

      // ETL Benchmarks
      results += benchmarkEtlOperations(taxiData, housingData, cpuSpark, "CPU", config)

      // ML Training Benchmarks
      results += benchmarkMlTraining(housingData, cpuSpark, "CPU", config)

      // Aggregation Benchmarks
      results += benchmarkAggregations(taxiData, cpuSpark, "CPU", config)

    } finally {
      cpuSpark.stop()
    }

    results.toSeq
  }

  /**
   * Executes GPU-optimized benchmarks.
   */
  private def executeGpuBenchmarks(dataset: BenchmarkDataset, config: BenchmarkConfig): Seq[BenchmarkResult] = {
    val results = ListBuffer[BenchmarkResult]()

    // Create GPU-optimized Spark session
    val gpuSpark = configuration.createOptimizedSparkSession(
      appName = "GPU-Benchmark",
      enableGpu = true
    )

    try {
      // Re-load data in GPU session
      val taxiData = gpuSpark.read.parquet("benchmark-data/taxi_trips").cache()
      val housingData = gpuSpark.read.parquet("benchmark-data/housing_market").cache()

      // ETL Benchmarks
      results += benchmarkEtlOperations(taxiData, housingData, gpuSpark, "GPU", config)

      // ML Training Benchmarks
      results += benchmarkMlTraining(housingData, gpuSpark, "GPU", config)

      // Aggregation Benchmarks
      results += benchmarkAggregations(taxiData, gpuSpark, "GPU", config)

    } finally {
      gpuSpark.stop()
    }

    results.toSeq
  }

  /**
   * Benchmarks ETL operations including filtering, joins, and transformations.
   */
  private def benchmarkEtlOperations(
    taxiData: DataFrame,
    housingData: DataFrame,
    spark: SparkSession,
    executionMode: String,
    config: BenchmarkConfig
  ): BenchmarkResult = {
    logger.info(s"Benchmarking ETL operations in $executionMode mode")

    val operationTimes = ListBuffer[Long]()

    // Warmup iterations
    (1 to config.warmupIterations).foreach { _ =>
      performEtlOperations(taxiData, housingData, spark)
    }

    // Actual benchmark iterations
    (1 to config.iterations).foreach { iteration =>
      logger.debug(s"ETL benchmark iteration $iteration/${config.iterations}")
      val startTime = System.currentTimeMillis()
      performEtlOperations(taxiData, housingData, spark)
      val executionTime = System.currentTimeMillis() - startTime
      operationTimes += executionTime
    }

    val avgTime = operationTimes.sum / operationTimes.length
    val minTime = operationTimes.min
    val maxTime = operationTimes.max

    BenchmarkResult(
      operationType = "ETL_Operations",
      executionMode = executionMode,
      datasetSize = taxiData.count() + housingData.count(),
      averageTimeMs = avgTime,
      minTimeMs = minTime,
      maxTimeMs = maxTime,
      iterations = config.iterations,
      throughputRecordsPerSecond = ((taxiData.count() + housingData.count()) * 1000.0 / avgTime).toLong
    )
  }

  /**
   * Performs actual ETL operations for benchmarking.
   */
  private def performEtlOperations(taxiData: DataFrame, housingData: DataFrame, spark: SparkSession): Unit = {
    // Complex filtering and aggregation
    val filteredTaxi = taxiData
      .filter(col("fare_amount") > 5.0)
      .filter(col("trip_distance_meters") > 1000)
      .groupBy("hour_of_day")
      .agg(
        avg("fare_amount").as("avg_fare"),
        sum("trip_distance_meters").as("total_distance"),
        count("*").as("trip_count")
      )

    // Force execution
    filteredTaxi.count()

    // Complex housing data transformations
    val processedHousing = housingData
      .withColumn("price_per_sqft", col("price") / col("square_feet"))
      .withColumn("room_efficiency", col("square_feet") / col("total_rooms"))
      .filter(col("price") > 100000)
      .groupBy("neighborhood")
      .agg(
        avg("price").as("avg_price"),
        avg("price_per_sqft").as("avg_price_per_sqft"),
        count("*").as("property_count")
      )

    // Force execution
    processedHousing.count()
  }

  /**
   * Creates output directory for benchmark results.
   */
  private def createOutputDirectory(path: String): Unit = {
    val dir = new File(path)
    if (!dir.exists()) {
      dir.mkdirs()
      logger.info(s"Created benchmark output directory: $path")
    }
  }

  /**
   * Logs comprehensive benchmark summary.
   */
  private def logBenchmarkSummary(results: BenchmarkResults): Unit = {
    logger.info("=== Benchmark Results Summary ===")
    logger.info(s"Total execution time: ${results.totalExecutionTimeMs}ms")
    logger.info(s"GPU available: ${results.gpuAvailable}")
    logger.info(s"Total benchmark operations: ${results.benchmarkResults.length}")

    // Group results by operation type and execution mode
    val groupedResults = results.benchmarkResults.groupBy(_.operationType)

    groupedResults.foreach { case (operationType, operationResults) =>
      logger.info(s"\n$operationType Results:")
      val cpuResults = operationResults.filter(_.executionMode == "CPU")
      val gpuResults = operationResults.filter(_.executionMode == "GPU")

      if (cpuResults.nonEmpty) {
        val avgCpuTime = cpuResults.map(_.averageTimeMs).sum / cpuResults.length
        logger.info(s"  CPU Average Time: ${avgCpuTime}ms")
      }

      if (gpuResults.nonEmpty) {
        val avgGpuTime = gpuResults.map(_.averageTimeMs).sum / gpuResults.length
        logger.info(s"  GPU Average Time: ${avgGpuTime}ms")

        if (cpuResults.nonEmpty) {
          val avgCpuTime = cpuResults.map(_.averageTimeMs).sum / cpuResults.length
          val speedup = avgCpuTime.toDouble / avgGpuTime
          logger.info(s"  GPU Speedup: ${speedup.formatted("%.2f")}x")
        }
      }
    }

    logger.info("=== Benchmark Summary Complete ===")
  }
}

// Data classes for benchmark results
case class BenchmarkDataset(
  taxiData: DataFrame,
  housingData: DataFrame,
  recordCount: Int,
  spark: SparkSession
)

case class BenchmarkResult(
  operationType: String,
  executionMode: String,
  datasetSize: Long,
  averageTimeMs: Long,
  minTimeMs: Long,
  maxTimeMs: Long,
  iterations: Int,
  throughputRecordsPerSecond: Long
)

case class BenchmarkResults(
  benchmarkResults: Seq[BenchmarkResult],
  totalExecutionTimeMs: Long,
  gpuAvailable: Boolean,
  config: PerformanceBenchmarkService.BenchmarkConfig
)