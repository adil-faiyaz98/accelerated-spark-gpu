# Benchmarking Module

The benchmarking module provides comprehensive performance testing capabilities for comparing CPU vs GPU execution in Apache Spark workloads.

## Purpose

This module enables quantitative analysis of GPU acceleration benefits by:
- Executing identical workloads on both CPU and GPU configurations
- Measuring execution times, throughput, and resource utilization
- Providing statistical analysis of performance improvements
- Supporting various data sizes and workload types

## Main Classes

### Performance Benchmarking
- **`PerformanceBenchmarkService`** - Core benchmarking orchestration
  - CPU vs GPU execution comparison
  - Multiple iteration support with warmup cycles
  - Configurable dataset sizes and operation types
  - Statistical analysis of results

### Benchmark Configuration
- **`BenchmarkConfig`** - Configuration for benchmark execution
  - Iteration counts and warmup settings
  - Dataset size specifications
  - GPU comparison enablement
  - Output directory configuration

### Result Data Classes
- **`BenchmarkResult`** - Individual benchmark operation results
- **`BenchmarkResults`** - Aggregated results across all operations
- **`BenchmarkDataset`** - Test data container for benchmarking

## Key Features

### Comprehensive Workload Coverage
- **ETL Operations**: Filtering, aggregations, joins, transformations
- **ML Training**: Model training with various algorithms
- **Data Processing**: Complex analytical queries and computations

### Statistical Rigor
- **Multiple Iterations**: Configurable iteration counts for statistical validity
- **Warmup Cycles**: JVM warmup to ensure accurate measurements
- **Performance Metrics**: Execution time, throughput, min/max/average statistics

### Flexible Configuration
```scala
val benchmarkConfig = PerformanceBenchmarkService.BenchmarkConfig(
  iterations = 5,
  warmupIterations = 2,
  datasetSizes = Seq(100000, 500000, 1000000),
  enableGpuComparison = true,
  outputDirectory = "benchmarks"
)
```

## Integration with Pipeline

The benchmarking module integrates with:

1. **Core Module** → Uses SparkGpuConfiguration for optimized sessions
2. **Core Module** → Leverages SyntheticDataGenerator for test data
3. **Reporting Module** → Provides results for performance reports

## Usage Examples

### Basic CPU vs GPU Benchmark
```scala
import com.enterprise.spark.gpu.benchmarking.PerformanceBenchmarkService

val benchmarkService = new PerformanceBenchmarkService()
val config = PerformanceBenchmarkService.BenchmarkConfig(
  iterations = 3,
  datasetSizes = Seq(100000, 500000),
  enableGpuComparison = true
)

val results = benchmarkService.executeComprehensiveBenchmarks(config)

// Analyze results
results.benchmarkResults.foreach { result =>
  println(s"${result.operationType} (${result.executionMode}): ${result.averageTimeMs}ms")
}
```

### Custom Benchmark Configuration
```scala
// Quick benchmark for development
val quickConfig = BenchmarkConfig(
  iterations = 2,
  warmupIterations = 1,
  datasetSizes = Seq(50000),
  enableGpuComparison = false  // CPU only for quick testing
)

// Production benchmark for comprehensive analysis
val prodConfig = BenchmarkConfig(
  iterations = 10,
  warmupIterations = 3,
  datasetSizes = Seq(1000000, 5000000, 10000000),
  enableGpuComparison = true,
  outputDirectory = "production-benchmarks"
)
```

### Analyzing Benchmark Results
```scala
val results = benchmarkService.executeComprehensiveBenchmarks(config)

// Calculate speedup metrics
val speedupAnalysis = results.benchmarkResults
  .groupBy(_.operationType)
  .map { case (operationType, operationResults) =>
    val cpuResults = operationResults.filter(_.executionMode == "CPU")
    val gpuResults = operationResults.filter(_.executionMode == "GPU")

    if (cpuResults.nonEmpty && gpuResults.nonEmpty) {
      val avgCpuTime = cpuResults.map(_.averageTimeMs).sum / cpuResults.length
      val avgGpuTime = gpuResults.map(_.averageTimeMs).sum / gpuResults.length
      val speedup = avgCpuTime.toDouble / avgGpuTime

      (operationType, speedup)
    } else {
      (operationType, 1.0)
    }
  }

speedupAnalysis.foreach { case (operation, speedup) =>
  println(f"$operation: ${speedup}%.2fx speedup")
}
```

## Benchmark Operations

### ETL Operations
- **Complex Filtering**: Multi-condition filters on large datasets
- **Aggregations**: GroupBy operations with multiple aggregation functions
- **Joins**: Various join types between large datasets
- **Window Functions**: Analytical window operations

### ML Training Operations
- **Random Forest**: Tree-based ensemble training
- **Linear Regression**: Gradient descent optimization
- **Feature Engineering**: Vector assembly and scaling operations

### Data Processing Operations
- **Statistical Computations**: Correlation, variance, percentile calculations
- **String Operations**: Text processing and pattern matching
- **Date/Time Operations**: Temporal data processing and aggregations

## Configuration Options

### Benchmark Parameters
```scala
case class BenchmarkConfig(
  iterations: Int = 3,              // Number of benchmark iterations
  warmupIterations: Int = 1,        // JVM warmup iterations
  datasetSizes: Seq[Int],          // Dataset sizes to test
  enableGpuComparison: Boolean,    // Enable GPU vs CPU comparison
  outputDirectory: String          // Results output directory
)
```

### RAPIDS Tuning Parameters
```scala
// GPU memory allocation
spark.config("spark.rapids.memory.gpu.allocFraction", "0.8")

// Concurrent GPU tasks
spark.config("spark.rapids.sql.concurrentGpuTasks", "2")

// GPU memory pooling
spark.config("spark.rapids.memory.gpu.pooling.enabled", "true")
```

## Best Practices

1. **Statistical Significance**: Use multiple iterations (3-5 minimum)
2. **Warmup Periods**: Include JVM warmup to avoid cold start bias
3. **Realistic Data Sizes**: Test with production-representative datasets
4. **Environment Isolation**: Run benchmarks in dedicated environments
5. **Baseline Consistency**: Maintain consistent CPU configurations for comparison

## Testing

Run benchmarking tests with:
```bash
sbt benchmarking/test
```