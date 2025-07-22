# GPU-Accelerated Apache Spark Pipeline

## Project Background

This project demonstrates the practical benefits of GPU acceleration in Apache Spark workloads. It provides a comprehensive example that helps organizations understand the real-world impact of GPU computing on data processing and machine learning workflows.

## Who This Project Helps

### Data Science Teams
- **Performance Validation**: Demonstrate GPU acceleration benefits with real data
- **Cost Justification**: Generate professional reports for infrastructure decisions
- **Technical Learning**: Understand GPU-accelerated ML pipeline patterns
- **Benchmarking**: Compare CPU vs GPU performance across workloads

### ML Engineering Teams
- **Pipeline Optimization**: Identify bottlenecks and optimization opportunities
- **Architecture Planning**: Production-ready multi-module design patterns
- **Performance Scaling**: Validate GPU acceleration benefits
- **Implementation Guide**: Complete RAPIDS integration example

### Business Stakeholders
- **ROI Analysis**: Professional reports with cost-benefit analysis
- **Investment Planning**: Data-driven decisions for GPU infrastructure
- **Executive Summaries**: Business-focused performance improvements
- **Risk Assessment**: Implementation complexity and expected returns

### DevOps Teams
- **Resource Planning**: GPU utilization patterns and infrastructure requirements
- **Cost Modeling**: Cloud pricing analysis for capacity planning
- **Deployment Validation**: End-to-end testing patterns
- **Performance Monitoring**: Comprehensive benchmarking frameworks

## Use Cases

### Financial Services
- **Risk Analytics**: Monte Carlo simulations and portfolio calculations
- **Fraud Detection**: Real-time ML model inference
- **Algorithmic Trading**: Feature engineering and model training

### Healthcare
- **Medical Imaging**: Image processing and computer vision
- **Drug Discovery**: Molecular simulation and compound analysis
- **Genomics**: Large-scale genomic datasets

### Retail
- **Recommendation Systems**: Collaborative filtering models
- **Demand Forecasting**: Historical sales data processing
- **Customer Segmentation**: Customer behavior analysis

### Manufacturing
- **Predictive Maintenance**: Industrial sensor data analysis
- **Quality Control**: Real-time defect detection
- **Supply Chain**: Logistics data processing

### Telecommunications
- **Network Optimization**: Traffic pattern analysis
- **Customer Churn**: Subscriber data processing
- **Fraud Detection**: Call pattern analysis

## Performance Benefits
- **Processing Speed**: 3-4x faster than CPU-only
- **Cost Reduction**: Significant compute time savings
- **Resource Efficiency**: Reduced compute hours required

### Business Impact
- **Faster Decision Making**: Accelerated analytics delivery
- **Competitive Advantage**: More frequent model updates
- **Cost Optimization**: Infrastructure cost savings
- **Innovation**: Faster experimentation cycles

## Documentation

| Document | Purpose |
|----------|---------|
| **[SETUP.md](SETUP.md)** | Setup and execution guide |
| **[PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md)** | Architecture and code organization |
| **[reports/LATEST_RESULTS.md](reports/LATEST_RESULTS.md)** | Performance results |
| **[models/MODEL_INFO.md](models/MODEL_INFO.md)** | Model metadata |
| **[screenshots/README.md](screenshots/README.md)** | Visual documentation |
| **[DOCUMENTATION.md](DOCUMENTATION.md)** | Navigation guide |

## Quick Start

```bash
# Windows
run.bat

# Linux/macOS
./run.sh

# Or using SBT directly
sbt "core/run"
```

## Performance Improvements

| Operation | CPU | GPU | Speedup |
|-----------|-----|-----|---------|
| ETL Processing | 45s | 12s | **3.7x** |
| ML Training | 121s | 28s | **4.3x** |
| Data Aggregation | 29s | 10s | **2.9x** |
| **Average** | - | - | **3.6x** |

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Core Module   │───▶│ Benchmarking     │───▶│  Reporting      │
│   ML Pipeline   │    │ CPU vs GPU       │    │  Professional   │
│   Data Gen      │    │ Performance      │    │  Reports        │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Integration    │    │ RAPIDS Accelerator│   │ Single Command  │
│  End-to-End     │    │  Spark Plugin    │    │ Execution       │
│  Testing        │    │  GPU Optimization│    │ System          │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Features

- **Single-Command Execution**: Complete workflow
- **Professional Reports**: HTML reports with charts
- **Comprehensive Testing**: Integration tests with real data
- **Multi-Module Architecture**: Separation of concerns
- **GPU Validation**: Automatic GPU detection
- **Performance Benchmarking**: CPU vs GPU comparisons

## System Requirements

### Minimum Requirements
- **Java 11+**: OpenJDK or Oracle JDK
- **SBT**: Scala Build Tool
- **8GB RAM**: For Spark operations
- **10GB Disk**: For datasets and models

### Optional (for GPU acceleration)
- **NVIDIA GPU**: CUDA Compute Capability 6.0+
- **CUDA**: GPU computing platform
- **8GB GPU Memory**: For optimal performance

### Installation

#### Windows
```bash
git clone https://github.com/adil-faiyaz98/accelerated-spark-gpu.git
cd accelerated-spark-gpu
./run.sh  # or run.bat on Windows
```

## Output

After running the pipeline:
```
data/                    # Generated datasets
models/                  # Trained ML models
reports/                 # Performance reports
sample-data/            # Sample datasets
```

## Configuration

### GPU Settings
```properties
spark.plugins=com.nvidia.spark.SQLPlugin
spark.rapids.sql.enabled=true
spark.rapids.memory.gpu.allocFraction=0.8
```

## Testing

```bash
sbt test
```

## License

MIT License - see [LICENSE](LICENSE) file for details.