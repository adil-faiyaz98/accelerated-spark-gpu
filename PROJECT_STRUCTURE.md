# Project Structure

## Architecture

```
accelerated-spark-gpu/
├── modules/
│   ├── core/                    # ML pipeline and data generation
│   ├── benchmarking/            # Performance testing
│   ├── reporting/               # Report generation
│   └── integration/             # End-to-end tests
├── conf/
│   └── spark-defaults.conf      # Spark configuration
├── data/                        # Generated datasets
├── models/                      # Trained ML models
├── reports/                     # Performance reports
├── sample-data/                 # Sample CSV files
└── src/main/scala/              # Main application code
```

## Modules

### Core
- ML pipeline and data generation
- Main application entry point
- Configuration management

### Benchmarking
- CPU vs GPU performance testing
- Performance metrics collection

### Reporting
- HTML report generation
- Cost analysis

### Integration
- End-to-end testing
- Workflow validation

## Key Files

- `build.sbt` - Build configuration
- `run.sh` / `run.bat` - Execution scripts
- `conf/spark-defaults.conf` - Spark configuration