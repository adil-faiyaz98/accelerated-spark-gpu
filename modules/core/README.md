# Core Module The core module contains the fundamental components of the GPU-accelerated Spark pipeline, including data generation, ML pipeline execution, and system configuration. ## Components ### Data Generation (`com.enterprise.spark.gpu.core.data`)
- **SyntheticDataGenerator**: Creates realistic synthetic datasets for benchmarking - Taxi trip data with temporal patterns and fare calculations - Housing market data with correlated features for ML training - Configurable record counts and validation options ### Pipeline Execution (`com.enterprise.spark.gpu.core.pipeline`)
- **MachineLearningPipelineExecutor**: Orchestrates the complete ML workflow - Data preprocessing and feature engineering - Model training (Random Forest, Linear Regression) - Model evaluation and persistence - Performance metrics collection ### Configuration (`com.enterprise.spark.gpu.core.config`)
- **SparkGpuConfiguration**: Centralized configuration management - GPU detection and automatic fallback - Performance optimization settings - Environment-specific configurations ### Application Entry Points
- **GpuPipelineApplication**: Unified single-command execution system
- **SparkGpuApplication**: Legacy entry point (deprecated) ## Key Features - **GPU Acceleration**: Automatic detection and configuration of RAPIDS SQL plugin
- **Synthetic Data**: Realistic datasets with statistical validity for benchmarking
- **ML Pipeline**: Complete workflow from data generation to model evaluation
- **Error Handling**: Comprehensive error handling and resource management
- **Logging**: Structured logging with performance metrics ## Usage ### Direct Module Usage
```scala
import com.enterprise.spark.gpu.core.pipeline.MachineLearningPipelineExecutor val executor = new MachineLearningPipelineExecutor()
val results = executor.executeComplete()
``` ### Configuration
```scala
import com.enterprise.spark.gpu.core.config.SparkGpuConfiguration val config = new SparkGpuConfiguration()
val spark = config.createOptimizedSparkSession("MyApp", enableGpu = true)
``` ### Data Generation
```scala
import com.enterprise.spark.gpu.core.data.SyntheticDataGenerator val generator = new SyntheticDataGenerator(spark)
val config = SyntheticDataGenerator.DataGenerationConfig( taxiRecordCount = 1000000, housingRecordCount = 500000
)
val summary = generator.generateAllDatasets(config)
``` ## Dependencies - Apache Spark 3.5.1
- RAPIDS 24.02.0 (for GPU acceleration)
- Scala 2.12.17
- ScalaTest 3.2.15 (for testing) ## Testing The module includes comprehensive tests:
- Unit tests for individual components
- Integration tests for complete workflows
- Performance validation tests
- GPU availability detection tests Run tests with:
```bash
sbt core/test
``` ## Configuration Files - `application.conf`: Application configuration settings
- `logback.xml`: Logging configuration
- Spark configuration applied programmatically based on GPU availability