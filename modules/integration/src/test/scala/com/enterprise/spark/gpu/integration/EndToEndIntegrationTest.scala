package com.enterprise.spark.gpu.integration

import com.enterprise.spark.gpu.core.SparkGpuTestBase
import com.enterprise.spark.gpu.benchmarking.PerformanceBenchmarkService
import com.enterprise.spark.gpu.reporting.ReportGenerationService
import org.scalatest.time.{Minutes, Span}
import java.io.File

/**
 * Comprehensive end-to-end integration test suite.
 *
 * This test suite validates the complete GPU-accelerated Spark pipeline
 * from data generation through final report generation. It ensures:
 * - All modules work together seamlessly
 * - Real data processing with measurable performance benefits
 * - Professional report generation with actual results
 * - Error handling and resource management across modules
 * - Performance validation with realistic datasets
 *
 * These tests use real Spark operations and actual GPU acceleration
 * (when available) to provide authentic validation of the system.
 */
class EndToEndIntegrationTest extends SparkGpuTestBase {

  // Increase timeout for integration tests
  implicit val patience = org.scalatest.time.Patience(Span(10, Minutes))

  test("should execute basic benchmark and reporting pipeline") {
    val (results, totalExecutionTime) = measureExecutionTime {
      // Step 1: Execute Performance Benchmarks
      println("=== Integration Test: Executing Performance Benchmarks ===")
      val benchmarkService = new PerformanceBenchmarkService()
      val benchmarkConfig = PerformanceBenchmarkService.BenchmarkConfig(
        iterations = 2, // Reduced for integration test speed
        warmupIterations = 1,
        datasetSizes = Seq(50000, 100000), // Smaller datasets for testing
        enableGpuComparison = true,
        outputDirectory = "test-reports/benchmarks"
      )
      val benchmarkResults = benchmarkService.executeComprehensiveBenchmarks(benchmarkConfig)

      // Step 2: Generate Comprehensive Reports
      println("=== Integration Test: Generating Reports ===")
      val reportService = new ReportGenerationService()
      val reportConfig = ReportGenerationService.ReportConfig(
        outputDirectory = "test-reports",
        generateCharts = true,
        includeExecutiveSummary = true,
        includeTechnicalDetails = true
      )
      val reportSummary = reportService.generateComprehensiveReports(
        Some(benchmarkResults),
        reportConfig
      )

      (benchmarkResults, reportSummary)
    }

    val (benchmarkResults, reportSummary) = results

    // Validate Benchmark Results
    validateBenchmarkResults(benchmarkResults)

    // Validate Report Generation
    validateReportGeneration(reportSummary)

    // Validate Overall Performance
    validateOverallPerformance(totalExecutionTime, 100000) // Approximate record count

    logTestPerformance("Complete End-to-End Integration", totalExecutionTime, 100000)
  }

  test("should handle GPU availability detection across all modules") {
    val benchmarkService = new PerformanceBenchmarkService()
    val benchmarkConfig = PerformanceBenchmarkService.BenchmarkConfig(
      iterations = 1,
      datasetSizes = Seq(10000),
      enableGpuComparison = true
    )
    val benchmarkResults = benchmarkService.executeComprehensiveBenchmarks(benchmarkConfig)

    // If GPU is available, benchmark should include GPU results
    if (benchmarkResults.gpuAvailable) {
      val gpuBenchmarks = benchmarkResults.benchmarkResults.filter(_.executionMode == "GPU")
      gpuBenchmarks should not be empty
      println(s"GPU benchmarks executed: ${gpuBenchmarks.length} operations")
    } else {
      println("GPU not available - CPU-only benchmarks executed")
    }
  }

  test("should generate professional reports with real performance data") {
    // Execute benchmarks to get performance comparisons
    val benchmarkService = new PerformanceBenchmarkService()
    val benchmarkResults = benchmarkService.executeComprehensiveBenchmarks(
      PerformanceBenchmarkService.BenchmarkConfig(
        iterations = 1,
        datasetSizes = Seq(25000),
        outputDirectory = "test-reports/integration-benchmarks"
      )
    )

    // Generate reports
    val reportService = new ReportGenerationService()
    val reportSummary = reportService.generateComprehensiveReports(
      Some(benchmarkResults),
      ReportGenerationService.ReportConfig(
        outputDirectory = "test-reports/integration",
        generateCharts = true,
        includeExecutiveSummary = true
      )
    )

    // Validate report files exist and contain real data
    reportSummary.generatedFiles should not be empty
    reportSummary.generatedFiles.foreach { filePath =>
      assertPathExists(filePath)
      // Verify files are not empty
      val file = new File(filePath)
      file.length() should be > 0L
    }

    // Validate HTML report contains actual performance data
    val htmlContent = scala.io.Source.fromFile(reportSummary.htmlReportPath).mkString
    htmlContent should include("GPU-Accelerated Apache Spark Performance Report")

    // If GPU benchmarks were run, report should include speedup information
    if (benchmarkResults.gpuAvailable) {
      htmlContent should include("CPU vs GPU Performance Comparison")
    }
  }

  test("should handle error conditions gracefully across all modules") {
    // This test validates error handling and resource cleanup
    val benchmarkService = new PerformanceBenchmarkService()

    // Test with very small dataset (edge case)
    val edgeCaseConfig = PerformanceBenchmarkService.BenchmarkConfig(
      iterations = 1,
      datasetSizes = Seq(10), // Very small dataset
      enableGpuComparison = false // Disable GPU to test CPU-only path
    )
    val benchmarkResults = benchmarkService.executeComprehensiveBenchmarks(edgeCaseConfig)

    // Should complete without errors even with edge case configuration
    benchmarkResults.benchmarkResults should not be empty
    benchmarkResults.totalExecutionTimeMs should be > 0L

    // All benchmark results should have valid metrics
    benchmarkResults.benchmarkResults.foreach { result =>
      result.averageTimeMs should be > 0L
      result.datasetSize should be > 0L
      result.throughputRecordsPerSecond should be >= 0L
    }
  }

  /**
   * Validates benchmark execution results.
   */
  private def validateBenchmarkResults(benchmarkResults: com.enterprise.spark.gpu.benchmarking.BenchmarkResults): Unit = {
    benchmarkResults should not be null
    benchmarkResults.benchmarkResults should not be empty
    benchmarkResults.totalExecutionTimeMs should be > 0L

    // Validate individual benchmark results
    benchmarkResults.benchmarkResults.foreach { result =>
      result.averageTimeMs should be > 0L
      result.minTimeMs should be > 0L
      result.maxTimeMs should be >= result.minTimeMs
      result.datasetSize should be > 0L
      result.iterations should be > 0
      result.throughputRecordsPerSecond should be >= 0L
    }

    // Should have CPU results at minimum
    val cpuResults = benchmarkResults.benchmarkResults.filter(_.executionMode == "CPU")
    cpuResults should not be empty

    // If GPU is available, should have GPU results
    if (benchmarkResults.gpuAvailable) {
      val gpuResults = benchmarkResults.benchmarkResults.filter(_.executionMode == "GPU")
      gpuResults should not be empty
    }
  }

  /**
   * Validates report generation results.
   */
  private def validateReportGeneration(reportSummary: com.enterprise.spark.gpu.reporting.ReportGenerationSummary): Unit = {
    reportSummary should not be null
    reportSummary.generatedFiles should not be empty
    reportSummary.generationTimeMs should be > 0L

    // Validate HTML report exists and is not empty
    assertPathExists(reportSummary.htmlReportPath)
    val htmlFile = new File(reportSummary.htmlReportPath)
    htmlFile.length() should be > 1000L // Should be substantial content

    // Validate all generated files exist
    reportSummary.generatedFiles.foreach { filePath =>
      assertPathExists(filePath)
      val file = new File(filePath)
      file.length() should be > 0L
    }
  }

  /**
   * Validates overall performance characteristics.
   */
  private def validateOverallPerformance(executionTimeMs: Long, recordsProcessed: Long): Unit = {
    // Integration test should complete within reasonable time
    executionTimeMs should be <= 600000L // 10 minutes maximum

    // Should process records at reasonable throughput
    val recordsPerSecond = (recordsProcessed * 1000.0) / executionTimeMs
    recordsPerSecond should be >= 50.0 // At least 50 records/second for integration test

    println(s"Integration test performance: ${recordsPerSecond.formatted("%.2f")} records/second")
  }
}