import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression._
import org.apache.spark.ml.Pipeline
import ml.dmlc.xgboost4j.scala.spark.XGBoostRegressor
import java.io.{File, PrintWriter}
import scala.collection.mutable.ListBuffer
import scala.util.{Random, Try, Success, Failure}

/**
 * GPU Benchmark suite for comparing CPU vs GPU performance
 * in Spark workloads including ETL, ML training, and aggregations.
 */
object GPUBenchmark {

  case class BenchmarkResult(
    operation: String,
    mode: String,
    executionTime: Float,
    dataSize: Long,
    throughput: Double
  )

  private val ETL_RECORDS = 1000000
  private val ML_RECORDS = 100000
  private val AGG_RECORDS = 500000
  private val RANDOM_SEED = 42L

  def main(args: Array[String]): Unit = {
    val results = ListBuffer[BenchmarkResult]()

    try {
      // CPU-only benchmark
      println("=== Running CPU-only Benchmark ===")
      val cpuResults = runBenchmark(enableGPU = false)
      results ++= cpuResults

      // GPU-accelerated benchmark
      println("=== Running GPU-accelerated Benchmark ===")
      val gpuResults = runBenchmark(enableGPU = true)
      results ++= gpuResults

      // Generate performance report
      generatePerformanceReport(results.toList)

    } catch {
      case e: Exception =>
        println(s"Benchmark failed: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    }
  }

  def runBenchmark(enableGPU: Boolean): List[BenchmarkResult] = {
    val mode = if (enableGPU) "GPU" else "CPU"
    val results = ListBuffer[BenchmarkResult]()

    val spark = createSparkSession(mode, enableGPU)
    import spark.implicits._

    try {
      // Benchmark 1: ETL Operations
      val (etlResult, etlTime) = timeOperation("ETL-Operations") {
        benchmarkETL(spark)
      }
      results += BenchmarkResult("ETL", mode, etlTime, etlResult, etlResult / etlTime)

      // Benchmark 2: ML Training
      val (mlResult, mlTime) = timeOperation("ML-Training") {
        benchmarkMLTraining(spark)
      }
      results += BenchmarkResult("ML-Training", mode, mlTime, mlResult, mlResult / mlTime)

      // Benchmark 3: Data Aggregation
      val (aggResult, aggTime) = timeOperation("Aggregation") {
        benchmarkAggregation(spark)
      }
      results += BenchmarkResult("Aggregation", mode, aggTime, aggResult, aggResult / aggTime)

    } finally {
      spark.stop()
    }

    results.toList
  }

  private def createSparkSession(mode: String, enableGPU: Boolean): SparkSession = {
    val sparkBuilder = SparkSession.builder()
      .appName(s"Benchmark-$mode")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    if (enableGPU) {
      sparkBuilder
        .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
        .config("spark.rapids.sql.enabled", "true")
        .config("spark.rapids.memory.gpu.pooling.enabled", "true")
        .config("spark.rapids.sql.concurrentGpuTasks", "2")
    }

    sparkBuilder.getOrCreate()
  }

  private def timeOperation[R](phase: String)(block: => R): (R, Float) = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    val elapsed = (t1 - t0).toFloat / 1000
    println(s"Elapsed time [$phase]: ${elapsed}s")
    (result, elapsed)
  }

  def benchmarkETL(spark: SparkSession): Long = {
    import spark.implicits._

    val random = new Random(RANDOM_SEED)

    // Generate larger dataset for benchmarking
    val largeData = (1 to ETL_RECORDS).map { i =>
      val category = s"cat_${i % 100}"
      val value1 = random.nextDouble() * 1000
      val value2 = random.nextDouble() * 500
      val timestamp = System.currentTimeMillis() + i * 1000
      (i, category, value1, value2, timestamp)
    }.toDF("id", "category", "value1", "value2", "timestamp")

    // Complex ETL operations
    val processed = largeData
      .filter($"value1" > 100)
      .withColumn("value_ratio", $"value1" / $"value2")
      .withColumn("value_sum", $"value1" + $"value2")
      .groupBy("category")
      .agg(
        avg("value1").as("avg_value1"),
        sum("value2").as("sum_value2"),
        count("*").as("record_count")
      )
      .filter($"record_count" > 5)

    processed.count()
  }

  def benchmarkMLTraining(spark: SparkSession): Long = {
    import spark.implicits._

    val random = new Random(RANDOM_SEED)

    // Generate ML training dataset
    val mlData = (1 to ML_RECORDS).map { i =>
      val features = Array.fill(10)(random.nextGaussian())
      val target = features.sum + random.nextGaussian() * 0.1
      (features(0), features(1), features(2), features(3), features(4),
       features(5), features(6), features(7), features(8), features(9), target)
    }.toDF("f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "target")

    val featureCols = Array("f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10")

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    val rf = new RandomForestRegressor()
      .setLabelCol("target")
      .setFeaturesCol("features")
      .setNumTrees(50)
      .setMaxDepth(8)
      .setSeed(RANDOM_SEED)

    val pipeline = new Pipeline().setStages(Array(assembler, rf))
    val model = pipeline.fit(mlData)

    mlData.count()
  }

  def benchmarkAggregation(spark: SparkSession): Long = {
    import spark.implicits._

    val random = new Random(RANDOM_SEED)

    val aggData = (1 to AGG_RECORDS).map { i =>
      val group = s"group_${i % 1000}"
      val subgroup = s"sub_${i % 100}"
      val metric1 = random.nextDouble() * 100
      val metric2 = random.nextInt(1000)
      (group, subgroup, metric1, metric2)
    }.toDF("group", "subgroup", "metric1", "metric2")

    val result = aggData
      .groupBy("group", "subgroup")
      .agg(
        avg("metric1").as("avg_metric1"),
        sum("metric2").as("sum_metric2"),
        stddev("metric1").as("stddev_metric1"),
        count("*").as("count")
      )
      .filter($"count" > 10)

    result.count()
  }

  def generatePerformanceReport(results: List[BenchmarkResult]): Unit = {
    ensureDirectoryExists("reports")

    val writer = new PrintWriter("reports/performance_report.md")

    try {
      writer.println("# GPU Acceleration Performance Report")
      writer.println()
      writer.println("## Executive Summary")
      writer.println()

      val cpuResults = results.filter(_.mode == "CPU")
      val gpuResults = results.filter(_.mode == "GPU")

      writer.println("| Operation | CPU Time (s) | GPU Time (s) | Speedup | Improvement |")
      writer.println("|-----------|--------------|--------------|---------|-------------|")

      var totalSpeedup = 0.0
      var operationCount = 0

      for (operation <- results.map(_.operation).distinct) {
        val cpuTime = cpuResults.find(_.operation == operation).map(_.executionTime).getOrElse(0.0f)
        val gpuTime = gpuResults.find(_.operation == operation).map(_.executionTime).getOrElse(0.0f)

        if (cpuTime > 0 && gpuTime > 0) {
          val speedup = cpuTime / gpuTime
          val improvement = ((cpuTime - gpuTime) / cpuTime * 100)
          totalSpeedup += speedup
          operationCount += 1
          writer.println(f"| $operation | $cpuTime%.2f | $gpuTime%.2f | ${speedup}%.2fx | ${improvement}%.1f%% |")
        }
      }

      val avgSpeedup = if (operationCount > 0) totalSpeedup / operationCount else 0.0
      writer.println()
      writer.println(f"**Average Speedup: ${avgSpeedup}%.2fx**")
      writer.println()

      // Cost Analysis
      generateCostAnalysis(writer, cpuResults, gpuResults)

      // Detailed Results
      generateDetailedResults(writer, results)

    } finally {
      writer.close()
    }

    println("Performance report generated: reports/performance_report.md")
  }

  private def generateCostAnalysis(writer: PrintWriter,
                                  cpuResults: List[BenchmarkResult],
                                  gpuResults: List[BenchmarkResult]): Unit = {
    writer.println("## Cost Analysis")
    writer.println()
    writer.println("### Infrastructure Costs (Hourly)")
    writer.println("- CPU Instance (16 cores): $2.50/hour")
    writer.println("- GPU Instance (V100): $8.00/hour")
    writer.println()

    val cpuTotalTime = cpuResults.map(_.executionTime).sum
    val gpuTotalTime = gpuResults.map(_.executionTime).sum
    val cpuCost = (cpuTotalTime / 3600) * 2.50
    val gpuCost = (gpuTotalTime / 3600) * 8.00
    val savings = cpuCost - gpuCost
    val savingsPercent = if (cpuCost > 0) (savings / cpuCost) * 100 else 0.0

    writer.println("### Workload Cost Comparison")
    writer.println(f"- CPU Total Runtime: ${cpuTotalTime}%.2f seconds")
    writer.println(f"- GPU Total Runtime: ${gpuTotalTime}%.2f seconds")
    writer.println(f"- CPU Cost: $$${cpuCost}%.4f")
    writer.println(f"- GPU Cost: $$${gpuCost}%.4f")
    writer.println(f"- **Cost Savings: $$${savings}%.4f (${savingsPercent}%.1f%%)**")
    writer.println()
  }

  private def generateDetailedResults(writer: PrintWriter, results: List[BenchmarkResult]): Unit = {
    writer.println("## Detailed Results")
    writer.println()
    writer.println("| Operation | Mode | Time (s) | Throughput (ops/s) |")
    writer.println("|-----------|------|----------|-------------------|")

    results.foreach { result =>
      writer.println(f"| ${result.operation} | ${result.mode} | ${result.executionTime}%.2f | ${result.throughput}%.2f |")
    }
  }

  private def ensureDirectoryExists(path: String): Unit = {
    val dir = new File(path)
    if (!dir.exists()) {
      dir.mkdirs()
    }
  }
}