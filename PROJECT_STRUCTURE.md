# Project Structure

## Architecture

```
accelerated-spark-gpu/
├── modules/
│   ├── core/                    # Configuration, data generation, base utilities
│   ├── benchmarking/            # Performance testing and CPU vs GPU comparison
│   ├── reporting/               # Professional report generation with charts
│   └── integration/             # End-to-end testing and validation
├── models/                      # Pre-trained ML models with metadata
├── conf/                        # Spark and RAPIDS configuration
│   ├── spark-defaults.conf      # Spark configuration
│   └── logging.conf             # Logging configuration
├── sample-data/                 # Sample datasets for quick testing
└── src/main/scala/              # Main application code
```

## Modules

### Core
- Synthetic data generation
- Main application entry point
- Configuration management
- Base classes and utilities

### Benchmarking
- CPU vs GPU performance testing
- Performance metrics collection
- Stress testing

### Reporting
- HTML report generation with charts
- Cost analysis
- Visualization tools

### Integration
- End-to-end testing
- Workflow validation
- Cross-platform compatibility checks

## Key Files

- `build.sbt` - Build configuration
- `run.sh` / `run.bat` - Execution scripts
- `conf/spark-defaults.conf` - Spark configuration
- `conf/logging.conf` - Logging configuration
