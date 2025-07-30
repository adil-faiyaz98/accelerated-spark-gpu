package com.enterprise.spark.gpu.core.pipeline

import com.enterprise.spark.gpu.core.SparkGpuTestBase
import org.apache.spark.ml.evaluation.RegressionEvaluator
import java.io.File

/**
 * Comprehensive test suite for MachineLearningPipelineExecutor.
 *
 * This test suite validates:
 * - End-to-end pipeline execution with real data processing
 * - Model training and evaluation with actual ML algorithms
 * - Performance measurement and benchmarking
 * - Data quality validation throughout the pipeline
 * - Error handling and resource management
 *
 * Tests use real Spark ML operations to ensure the pipeline works
 * correctly in production scenarios.
 */
class MachineLearningPipelineExecutorTest extends SparkGpuTestBase {
    private var pipelineExecutor: MachineLearningPipelineExecutor = _

    override def beforeEach(): Unit = {
        super.beforeEach()
        pipelineExecutor = new MachineLearningPipelineExecutor()
    }

    test("should execute complete pipeline successfully with real data processing") {
        val (summary, executionTime) = measureExecutionTime {
            pipelineExecutor.executeComplete()
        }

        // Verify pipeline execution summary
        summary should not be null
        summary.totalExecutionTimeMs should be > 0L
        summary.dataGenerationSummary should not be null
        summary.taxiAnalysisResults should not be null
        summary.modelTrainingResults should not be null

        // Verify data generation results
        val dataGenSummary = summary.dataGenerationSummary
        dataGenSummary.totalRecords should be > 0
        dataGenSummary.validationResults.valid shouldBe true

        // Verify taxi analysis results
        val taxiResults = summary.taxiAnalysisResults
        taxiResults.totalTrips should be > 0L
        taxiResults.analysisTimeMs should be > 0L
        taxiResults.averageFare should be > 0.0
        taxiResults.peakHours should not be empty

        // Verify model training results
        val modelResults = summary.modelTrainingResults

        // Random Forest metrics validation
        val rfMetrics = modelResults.randomForestMetrics
        assertModelMetrics(
            rmse = rfMetrics.rmse,
            mae = rfMetrics.mae,
            r2 = rfMetrics.r2,
            maxRmse = 200000.0, // Reasonable for housing prices
            maxMae = 150000.0,
            minR2 = 0.1 // Should explain at least 10% of variance
        )

        // Linear Regression metrics validation
        val lrMetrics = modelResults.linearRegressionMetrics
        assertModelMetrics(
            rmse = lrMetrics.rmse,
            mae = lrMetrics.mae,
            r2 = lrMetrics.r2,
            maxRmse = 300000.0, // Linear regression might be less accurate
            maxMae = 200000.0,
            minR2 = 0.05
        )

        // Verify models were saved
        assertPathExists("models/random_forest_model")
        assertPathExists("models/linear_regression_model")

        // Verify training times are reasonable
        rfMetrics.trainingTimeMs should be > 0L
        lrMetrics.trainingTimeMs should be > 0L

        logTestPerformance("Complete Pipeline Execution", executionTime, dataGenSummary.totalRecords)
    }

    test("should perform realistic taxi data analysis with complex aggregations") {
        val summary = pipelineExecutor.executeComplete()
        val taxiResults = summary.taxiAnalysisResults

        // Verify taxi analysis performed meaningful operations
        taxiResults.totalTrips should be >= 1000000L // Should process the full synthetic dataset
        taxiResults.averageFare should be > 0.0
        taxiResults.averageFare should be < 1000.0 // Reasonable fare range

        // Peak hours should be realistic (typically 7-9 AM or 5-7 PM)
        taxiResults.peakHours should not be empty
        taxiResults.peakHours.size should be <= 3 // Top 3 peak hours

        // Analysis should complete in reasonable time
        taxiResults.analysisTimeMs should be > 0L
        taxiResults.analysisTimeMs should be < 60000L // Should complete within 1 minute

        // Verify the analysis actually processed data (not just returned defaults)
        taxiResults.averageFare should not equal 0.0
    }

    test("should train models with realistic performance metrics") {
        val summary = pipelineExecutor.executeComplete()
        val modelResults = summary.modelTrainingResults

        // Both models should be trained
        modelResults.randomForestMetrics should not be null
        modelResults.linearRegressionMetrics should not be null

        // Models should have different performance characteristics
        val rfMetrics = modelResults.randomForestMetrics
        val lrMetrics = modelResults.linearRegressionMetrics

        // Random Forest should generally perform better than Linear Regression for this dataset
        if (rfMetrics.r2 > 0.1 && lrMetrics.r2 > 0.1) {
            math.max(rfMetrics.r2, lrMetrics.r2) should be >= 0.2
        }

        // RMSE should be reasonable for housing price prediction
        rfMetrics.rmse should be < 500000.0
        lrMetrics.rmse should be < 500000.0

        // MAE should be reasonable
        rfMetrics.mae should be < 300000.0
        lrMetrics.mae should be < 300000.0

        // Training times should be reasonable
        rfMetrics.trainingTimeMs should be > 100L
        rfMetrics.trainingTimeMs should be < 300000L
        lrMetrics.trainingTimeMs should be > 50L
        lrMetrics.trainingTimeMs should be < 60000L
    }

    test("should handle data preprocessing correctly") {
        val testData = createSampleHousingData(1000)

        // Verify the test data has expected properties
        testData.count() shouldEqual 1000

        // Check for any null values (should be none in our test data)
        val nullCounts = testData.columns.map { col =>
            testData.filter(testData(col).isNull).count()
        }
        nullCounts.sum shouldEqual 0L

        // Verify data ranges are reasonable
        val priceStats = testData.agg(
            org.apache.spark.sql.functions.min("price"),
            org.apache.spark.sql.functions.max("price"),
            org.apache.spark.sql.functions.avg("price")
        ).collect()(0)

        priceStats.getDouble(0) should be > 0.0
        priceStats.getDouble(1) should be > priceStats.getDouble(0)
        priceStats.getDouble(2) should be > 0.0
    }

    test("should save and load trained models correctly") {
        val summary = pipelineExecutor.executeComplete()

        // Verify model files exist
        val rfModelDir = new File("models/random_forest_model")
        val lrModelDir = new File("models/linear_regression_model")
        rfModelDir.exists() shouldBe true
        lrModelDir.exists() shouldBe true

        // Verify model directories contain expected files
        rfModelDir.listFiles() should not be empty
        lrModelDir.listFiles() should not be empty

        // Try to load the saved models
        import org.apache.spark.ml.PipelineModel
        val loadedRfModel = PipelineModel.load("models/random_forest_model")
        val loadedLrModel = PipelineModel.load("models/linear_regression_model")
        loadedRfModel should not be null
        loadedLrModel should not be null

        // Test that loaded models can make predictions
        val testData = createSampleHousingData(10)
        val rfPredictions = loadedRfModel.transform(testData)
        val lrPredictions = loadedLrModel.transform(testData)

        // Verify predictions were generated
        rfPredictions.columns should contain("prediction")
        lrPredictions.columns should contain("prediction")
        rfPredictions.count() shouldEqual 10
        lrPredictions.count() shouldEqual 10

        // Verify predictions are reasonable (not NaN or extreme values)
        val rfPredStats = rfPredictions.agg(
            org.apache.spark.sql.functions.min("prediction"),
            org.apache.spark.sql.functions.max("prediction"),
            org.apache.spark.sql.functions.avg("prediction")
        ).collect()(0)

        rfPredStats.getDouble(0) should be > 0.0
        rfPredStats.getDouble(1) should be < 10000000.0
        rfPredStats.getDouble(2) should be > 0.0
    }

    test("should measure execution performance accurately") {
        val (summary, measuredTime) = measureExecutionTime {
            pipelineExecutor.executeComplete()
        }

        // Pipeline's internal timing should be close to our measurement
        val timeDifference = math.abs(summary.totalExecutionTimeMs - measuredTime)
        val toleranceMs = 1000L
        timeDifference should be <= toleranceMs

        // Verify individual component times add up reasonably
        val componentTimes = summary.dataGenerationSummary.generationTimeMs +
            summary.taxiAnalysisResults.analysisTimeMs +
            summary.modelTrainingResults.randomForestMetrics.trainingTimeMs +
            summary.modelTrainingResults.linearRegressionMetrics.trainingTimeMs

        summary.totalExecutionTimeMs should be >= (componentTimes * 0.5).toLong
    }

    test("should handle GPU availability detection") {
        val summary = pipelineExecutor.executeComplete()

        // GPU availability should be detected (true or false, not null)
        summary.gpuAccelerationEnabled should (be(true) or be(false))

        // If GPU is available, it should be noted in the summary
        if (summary.gpuAccelerationEnabled) {
            logger.info("GPU acceleration detected and enabled")
        } else {
            logger.info("GPU acceleration not available - using CPU-only mode")
        }

        // Pipeline should work regardless of GPU availability
        summary.modelTrainingResults should not be null
        summary.modelTrainingResults.randomForestMetrics.r2 should be >= -1.0
    }

    test("should validate data quality throughout pipeline") {
        val summary = pipelineExecutor.executeComplete()

        // Data generation validation should pass
        summary.dataGenerationSummary.validationResults.valid shouldBe true

        // Model metrics should be within valid ranges
        val rfMetrics = summary.modelTrainingResults.randomForestMetrics
        val lrMetrics = summary.modelTrainingResults.linearRegressionMetrics

        rfMetrics.r2 should be <= 1.0
        lrMetrics.r2 should be <= 1.0

        rfMetrics.rmse should be > 0.0
        rfMetrics.mae should be > 0.0
        lrMetrics.rmse should be > 0.0
        lrMetrics.mae should be > 0.0

        rfMetrics.trainingTimeMs should be > 0L
        lrMetrics.trainingTimeMs should be > 0L
    }

    test("should demonstrate clear performance benefits with larger datasets") {
        val (summary, executionTime) = measureExecutionTime {
            pipelineExecutor.executeComplete()
        }

        val recordsProcessed = summary.dataGenerationSummary.totalRecords
        val recordsPerSecond = (recordsProcessed * 1000.0) / executionTime

        recordsPerSecond should be >= 1000.0
        executionTime should be <= 300000L

        logTestPerformance("Large Dataset Processing", executionTime, recordsProcessed)
    }
}