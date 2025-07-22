# Benchmarking Module The benchmarking module provides comprehensive performance testing capabilities for comparing CPU and GPU execution in Apache Spark workloads. ## Components ### Performance Benchmarking (`com.enterprise.spark.gpu.benchmarking`)
- **PerformanceBenchmarkService**: Core benchmarking service - CPU vs GPU performance comparisons - ETL operation benchmarks - Machine learning training speed tests - Complex aggregation performance analysis - Statistical analysis with multiple iterations ## Key Features ### Benchmark Operations
- **ETL Operations**: Filtering, transformations, and complex data processing
- **ML Training**: Model training performance with different algorithms
- **Aggregations**: Complex multi-dimensional aggregations and calculations
- **Memory Usage**: Resource utilization monitoring ### Performance Metrics
- **Execution Time**: Average, minimum, and maximum execution times
- **Throughput**: Records processed per second
- **Speedup Analysis**: CPU vs GPU performance ratios
- **Statistical Validation**: Multiple iterations with warmup periods ### Configuration Options
- **Dataset Sizes**: Configurable data volumes for scaling analysis
- **Iterations**: Multiple runs for statistical significance
- **Warmup**: Warmup iterations to eliminate JVM startup effects
- **GPU Comparison**: Optional GPU benchmarking based on availability ## Usage ### Basic Benchmarking
```scala
import com.enterprise.spark.gpu.benchmarking.PerformanceBenchmarkService val benchmarkService = new PerformanceBenchmarkService()
val config = PerformanceBenchmarkService.BenchmarkConfig( iterations = 3, warmupIterations = 1, datasetSizes = Seq(100000, 500000, 1000000), enableGpuComparison = true
) val results = benchmarkService.executeComprehensiveBenchmarks(config)
``` ### Custom Configuration
```scala
val customConfig = PerformanceBenchmarkService.BenchmarkConfig( iterations = 5, warmupIterations = 2, datasetSizes = Seq(1000000, 5000000), enableGpuComparison = true, outputDirectory = "custom-benchmarks"
)
``` ### Results Analysis
```scala
// Access benchmark results
results.benchmarkResults.foreach { result => println(s"Operation: ${result.operationType}") println(s"Mode: ${result.executionMode}") println(s"Average Time: ${result.averageTimeMs}ms") println(s"Throughput: ${result.throughputRecordsPerSecond} records/sec")
} // Calculate speedup metrics
val speedups = results.benchmarkResults .groupBy(_.operationType) .map { case (op, results) => val cpu = results.filter(_.executionMode == "CPU").head val gpu = results.filter(_.executionMode == "GPU").head op -> (cpu.averageTimeMs.toDouble / gpu.averageTimeMs) }
``` ## Benchmark Types ### ETL Benchmarks
- Complex filtering operations
- Multi-table joins
- Data transformations
- Column calculations
- Aggregation operations ### ML Training Benchmarks
- Random Forest training
- Linear Regression training
- Feature engineering pipelines
- Model evaluation ### Aggregation Benchmarks
- Multi-dimensional grouping
- Statistical calculations
- Percentile computations
- Time-series analysis ## Performance Validation The benchmarking module includes validation to ensure:
- Consistent results across iterations
- Realistic performance metrics
- Proper GPU utilization when available
- Statistical significance of results ## Dependencies - Core module (for data generation and configuration)
- Apache Spark MLlib
- Scala CSV library for result export ## Testing Comprehensive test suite includes:
- Benchmark execution validation
- Performance metric accuracy
- GPU availability handling
- Error condition testing Run tests with:
```bash
sbt benchmarking/test
``` ## Output Benchmark results are saved in configurable formats:
- Structured benchmark result objects
- Performance logs with detailed metrics
- CSV export for further analysis
- Integration with reporting module for visualization