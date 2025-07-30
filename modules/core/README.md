# Core Module

The core module provides the foundational components for GPU-accelerated Spark applications, including configuration management, data generation, and pipeline orchestration.

## Purpose

This module serves as the backbone of the GPU-accelerated Spark pipeline, providing:
- Centralized configuration management for GPU/CPU execution modes
- Synthetic data generation for benchmarking and testing
- Base classes and utilities for Spark applications
- Enterprise-grade application entry points

## Main Classes

### Configuration Management
- **`SparkGpuConfiguration`** - Centralized configuration for GPU-accelerated Spark sessions
  - Automatic GPU detection and fallback to CPU
  - Environment-specific settings (dev, test, prod)
  - Performance optimization based on available resources

### Data Generation
- **`SyntheticDataGenerator`** - Generates realistic datasets for benchmarking
  - Taxi trip data with temporal patterns and fare calculations
  - Housing market data with correlated features for ML training
  - Configurable record counts and output formats

### Application Entry Points
- **`GpuPipelineApplication`** - Enterprise application for single-command execution
  - Complete workflow orchestration
  - Command-line argument parsing
  - Resource management and cleanup

### Test Infrastructure
- **`SparkGpuTestBase`** - Base class for all Spark tests
  - Shared Spark session management
  - Common test utilities and assertions
  - Performance measurement helpers

## Key Features

### GPU Acceleration Support
```scala
val config = new SparkGpuConfiguration()
val spark = config.createOptimizedSparkSession(
  appName = "MyApp",
  enableGpu = true  // Automatically detects GPU availability
)
```

### Realistic Data Generation
```scala
val generator = new SyntheticDataGenerator(spark)
val config = SyntheticDataGenerator.DataGenerationConfig(
  taxiRecordCount = 1000000,
  housingRecordCount = 500000,
  outputDirectory = "data"
)
val summary = generator.generateAllDatasets(config)
```

### Enterprise Application Execution
```scala
// Single command execution
sbt "run"                    // Complete workflow
sbt "run --quick"            // Quick demo with smaller datasets
sbt "run --benchmark-only"   // Performance benchmarks only
sbt "run --no-gpu"           // Force CPU-only execution
```

## Integration with Pipeline

The core module integrates with other modules as follows:

1. **Configuration** → Provides optimized Spark sessions to all modules
2. **Data Generation** → Creates datasets used by benchmarking and reporting modules
3. **Test Infrastructure** → Supports testing across all modules
4. **Application Entry** → Orchestrates execution of benchmarking and reporting modules

## Dependencies

- Apache Spark 3.5.1
- RAPIDS Accelerator for Apache Spark 24.02.0
- Scala 2.12.17
- ScalaTest 3.2.15 (for testing)

## Configuration Options

### GPU Settings
```scala
// Automatic GPU detection
val gpuAvailable = configuration.isGpuAvailable()

// GPU-specific configurations
spark.config("spark.rapids.sql.enabled", "true")
spark.config("spark.rapids.memory.gpu.pooling.enabled", "true")
spark.config("spark.rapids.memory.gpu.allocFraction", "0.8")
```

### Performance Tuning
```scala
// Adaptive Query Execution
spark.config("spark.sql.adaptive.enabled", "true")
spark.config("spark.sql.adaptive.coalescePartitions.enabled", "true")

// Memory optimization
spark.config("spark.executor.memoryFraction", "0.8")
spark.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```

## Usage Examples

### Basic GPU-Accelerated Pipeline
```scala
import com.enterprise.spark.gpu.core.config.SparkGpuConfiguration
import com.enterprise.spark.gpu.core.data.SyntheticDataGenerator

// Create optimized Spark session
val config = new SparkGpuConfiguration()
val spark = config.createOptimizedSparkSession("MyPipeline")

// Generate test data
val generator = new SyntheticDataGenerator(spark)
val dataConfig = SyntheticDataGenerator.DataGenerationConfig(
  taxiRecordCount = 100000,
  housingRecordCount = 50000
)
val summary = generator.generateAllDatasets(dataConfig)

// Use generated data for ML pipeline
val housingData = spark.read.parquet(summary.housingFilePath)
// ... continue with ML operations
```

### Testing with Core Infrastructure
```scala
import com.enterprise.spark.gpu.core.SparkGpuTestBase

class MyPipelineTest extends SparkGpuTestBase {
  test("should process housing data correctly") {
    val testData = createSampleHousingData(1000)

    val result = testData
      .filter(col("price") > 100000)
      .groupBy("neighborhood")
      .agg(avg("price").as("avg_price"))

    assertRecordCount(result, 5)
    // ... additional assertions
  }
}
```

## Best Practices

1. **Resource Management**: Always use the provided configuration classes for consistent resource management
2. **GPU Detection**: Let the configuration automatically detect GPU availability rather than hardcoding
3. **Data Generation**: Use realistic data generation for meaningful benchmarks
4. **Testing**: Extend SparkGpuTestBase for consistent test infrastructure
5. **Error Handling**: Leverage the built-in error handling and logging capabilities

## Performance Considerations

- **GPU Memory**: Configure `spark.rapids.memory.gpu.allocFraction` based on available GPU memory
- **Parallelism**: Adjust `spark.sql.shuffle.partitions` based on cluster size and data volume
- **Caching**: Use DataFrame caching judiciously for frequently accessed datasets
- **Data Formats**: Prefer Parquet format for optimal GPU acceleration performance