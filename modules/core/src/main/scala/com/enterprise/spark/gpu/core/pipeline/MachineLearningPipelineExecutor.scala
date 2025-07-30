package com.enterprise.spark.gpu.core.pipeline

import com.enterprise.spark.gpu.core.config.SparkGpuConfiguration
import com.enterprise.spark.gpu.core.data.{DataGenerationSummary, SyntheticDataGenerator}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.ml.regression.{LinearRegression, RandomForestRegressor}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.io.{File, FileWriter, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Machine Learning Pipeline Executor for GPU acceleration demonstration.
 *
 * This class orchestrates the complete ML workflow including:
 * - Synthetic data generation with realistic patterns
 * - Feature engineering and data preprocessing 
 * - Model training with both CPU and GPU-optimized algorithms
 * - Model evaluation and performance metrics
 * - Comprehensive logging and progress tracking
 *
 * The pipeline is designed to showcase measurable GPU acceleration benefits
 * in real-world ML scenarios while maintaining code quality.
 */
class MachineLearningPipelineExecutor extends LazyLogging {

  private var sparkSession: Option[SparkSession] = None
  private val configuration = new SparkGpuConfiguration()

  /**
   * Executes the complete ML pipeline workflow.
   *
   * This method runs the full end-to-end pipeline:
   * 1. Initialize optimized Spark session with GPU detection
   * 2. Generate synthetic datasets for training and evaluation
   * 3. Perform feature engineering and data preprocessing
   * 4. Train multiple ML models (Random Forest, Linear Regression)
   * 5. Evaluate model performance and generate metrics
   * 6. Save trained models for later use
   *
   * @return Pipeline execution summary with performance metrics
   */
  def executeComplete(): PipelineExecutionSummary = {
    logger.info("=== Starting Complete ML Pipeline Execution ===")
    val startTime = System.currentTimeMillis()

    try {
      // Step 1: Initialize Spark session with optimal configuration
      logger.info("Step 1: Initializing Spark session with GPU optimization")
      val spark = initializeSparkSession()

      // Step 2: Generate synthetic training data  
      logger.info("Step 2: Generating synthetic datasets")
      val dataGenerator = new SyntheticDataGenerator(spark)
      val dataGenerationSummary = dataGenerator.generateAllDatasets()

      // Step 3: Load and prepare housing data for ML training
      logger.info("Step 3: Loading and preparing housing dataset for ML training")
      val housingData = loadHousingDataset(spark, dataGenerationSummary.housingFilePath)
      val (trainingData, testData) = prepareTrainingData(housingData)

      // Step 4: Execute taxi data analysis (ETL demonstration)
      logger.info("Step 4: Executing taxi data ETL analysis") 
      val taxiAnalysisResults = executeTaxiAnalysis(spark, dataGenerationSummary.taxiFilePath)

      // Step 5: Train ML models
      logger.info("Step 5: Training machine learning models")
      val modelTrainingResults = trainMachineLearningModels(trainingData, testData)

      // Step 6: Save trained models
      logger.info("Step 6: Saving trained models")
      saveTrainedModels(modelTrainingResults)

      val totalExecutionTime = System.currentTimeMillis() - startTime
      val summary = PipelineExecutionSummary(
        dataGenerationSummary = dataGenerationSummary,
        taxiAnalysisResults = taxiAnalysisResults,
        modelTrainingResults = modelTrainingResults,
        totalExecutionTimeMs = totalExecutionTime,
        gpuAccelerationEnabled = configuration.isGpuAvailable()
      )

      logger.info(s"=== Pipeline Execution Completed Successfully in ${totalExecutionTime}ms ===")
      logExecutionSummary(summary)
      summary

    } catch {
      case e: Exception =>
        logger.error("Pipeline execution failed", e)
        throw new RuntimeException(s"ML Pipeline execution failed: ${e.getMessage}", e)
    } finally {
      cleanupResources()
    }
  }

  private def initializeSparkSession(): SparkSession = {
    val spark = configuration.createOptimizedSparkSession(
      appName = "GPU-Accelerated ML Pipeline",
      enableGpu = true
    )
    sparkSession = Some(spark)
    spark
  }

  private def loadHousingDataset(spark: SparkSession, dataPath: String): DataFrame = {
    logger.info(s"Loading housing dataset from: $dataPath")
    val housingDF = spark.read.parquet(dataPath)
    logger.info(s"Loaded housing dataset with ${housingDF.count()} records and ${housingDF.columns.length} columns")
    logger.info(s"Dataset schema: ${housingDF.columns.mkString(", ")}")
    housingDF.cache()
    housingDF
  }

  private def prepareTrainingData(housingData: DataFrame): (DataFrame, DataFrame) = {
    logger.info("Preparing training and test datasets")
    
    // Remove any records with null values
    val cleanData = housingData.na.drop()
    
    // Log data quality metrics
    val originalCount = housingData.count()
    val cleanCount = cleanData.count()
    logger.info(s"Data cleaning: ${originalCount - cleanCount} records removed " +
      s"(${((originalCount - cleanCount).toDouble / originalCount * 100).formatted("%.2f")}%)")
    
    // Split data into training (80%) and test (20%) sets
    val Array(trainingData, testData) = cleanData.randomSplit(Array(0.8, 0.2), seed = 42)
    logger.info(s"Training set: ${trainingData.count()} records")
    logger.info(s"Test set: ${testData.count()} records")
    
    // Cache both datasets
    trainingData.cache()
    testData.cache()
    (trainingData, testData)
  }

  // Rest of the implementation...
}

case class PipelineExecutionSummary(
  dataGenerationSummary: DataGenerationSummary,
  taxiAnalysisResults: TaxiAnalysisResults,
  modelTrainingResults: ModelTrainingResults,
  totalExecutionTimeMs: Long,
  gpuAccelerationEnabled: Boolean
)

case class TaxiAnalysisResults(
  totalTrips: Long,
  analysisTimeMs: Long,
  averageFare: Double,
  peakHours: Seq[Int]
)

case class ModelTrainingResults(
  randomForestMetrics: ModelMetrics,
  linearRegressionMetrics: ModelMetrics,
  rfModel: PipelineModel,
  lrModel: PipelineModel
)

case class ModelMetrics(
  modelType: String,
  rmse: Double,
  mae: Double,
  r2: Double,
  trainingTimeMs: Long
)
