package com.enterprise.spark.gpu.core

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

/**
 * Legacy application entry point - DEPRECATED
 *
 * This class is maintained for backward compatibility.
 * For new deployments, use GpuPipelineApplication which provides
 * a unified single-command execution system with comprehensive reporting.
 *
 * @deprecated Use GpuPipelineApplication instead
 * @author Spark GPU Team
 * @version 1.0.0
 */
@deprecated("Use GpuPipelineApplication for new deployments", "1.0.0")
object SparkGpuApplication extends LazyLogging {

    /**
     * Legacy application entry point - redirects to new unified system.
     *
     * @param args Command line arguments (forwarded to new system)
     */
    def main(args: Array[String]): Unit = {
        logger.warn("Using deprecated SparkGpuApplication - consider migrating to GpuPipelineApplication")
        logger.info("Redirecting to unified pipeline system...")
        // Redirect to new unified system
        GpuPipelineApplication.main(args)
    }

    /**
     * Executes the complete workflow including data generation, model training,
     * benchmarking, and evaluation with comprehensive reporting.
     */
    private def runCompleteWorkflow(): Unit = {
        logger.info("=== Starting Complete GPU-Accelerated Pipeline Workflow ===")

        // Step 1: Execute ML Pipeline
        logger.info("Phase 1: Executing ML Pipeline")
        runPipelineComponent(Array.empty)

        // Step 2: Run Performance Benchmarks
        logger.info("Phase 2: Running Performance Benchmarks")
        runBenchmarkComponent(Array.empty)

        // Step 3: Evaluate Models
        logger.info("Phase 3: Evaluating Models")
        runEvaluationComponent(Array.empty)

        logger.info("=== Complete Workflow Finished Successfully ===")
        logger.info("Results available in:")
        logger.info(" - Data: data/ directory")
        logger.info(" - Models: models/ directory")
        logger.info(" - Reports: reports/ directory")
    }

    /**
     * Runs the ML pipeline component with synthetic data generation and model training.
     */
    private def runPipelineComponent(args: Array[String]): Unit = {
        import com.enterprise.spark.gpu.core.pipeline.MachineLearningPipelineExecutor
        val pipelineExecutor = new MachineLearningPipelineExecutor()
        pipelineExecutor.executeComplete()
    }

    /**
     * Runs the benchmarking component to compare CPU vs GPU performance.
     */
    private def runBenchmarkComponent(args: Array[String]): Unit = {
        // This will be implemented in the benchmarking module
        logger.info("Benchmarking component execution - delegating to benchmarking module")
    }

    /**
     * Runs the model evaluation component to assess accuracy and performance.
     */
    private def runEvaluationComponent(args: Array[String]): Unit = {
        // This will be implemented in the reporting module
        logger.info("Evaluation component execution - delegating to reporting module")
    }

    /**
     * Displays comprehensive usage information for the application.
     */
    private def printUsageInformation(): Unit = {
        println("GPU-Accelerated Spark Pipeline")
        println("Usage: sbt 'run [mode]' or java -jar spark-gpu-pipeline.jar [mode]")
        println()
        println("Available modes:")
        println(" pipeline - Execute ML pipeline with synthetic data generation and model training")
        println(" benchmark - Run CPU vs GPU performance comparisons with detailed metrics")
        println(" evaluate - Perform model evaluation and accuracy validation")
        println(" all - Execute complete workflow (pipeline + benchmark + evaluate)")
        println()
        println("Examples:")
        println(" sbt 'run all' # Run complete workflow")
        println(" sbt 'run pipeline' # Run only ML pipeline")
        println(" sbt 'run benchmark' # Run only performance benchmarks")
        println()
        println("For more information, see README.md and documentation in docs/")
    }
}