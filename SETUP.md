# Setup Guide

## Quick Start

```bash
git clone https://github.com/adil-faiyaz98/accelerated-spark-gpu.git
cd accelerated-spark-gpu
./run.sh  # or run.bat on Windows
```

## Prerequisites

### Required
- Java 11+
- SBT (Scala Build Tool)
- 8GB+ RAM

### Optional (for GPU acceleration)
- NVIDIA GPU with CUDA support
- CUDA Toolkit

## Installation

### Install Java
```bash
# Ubuntu/Debian
sudo apt install openjdk-11-jdk

# macOS
brew install openjdk@11

# Windows - download from adoptium.net
```

### Install SBT

```bash
# Ubuntu/Debian
sudo apt install sbt

# macOS
brew install sbt

# Windows - download from scala-sbt.org
```

### Install CUDA Toolkit (Optional for GPU Acceleration)

```bash
# Ubuntu/Debian
sudo apt install nvidia-cuda-toolkit

# macOS
# CUDA is not natively supported; use an external GPU setup if required.

# Windows
# Download and install from https://developer.nvidia.com/cuda-downloads
```

## Running the Pipeline

```bash
# Clone repository
git clone https://github.com/adil-faiyaz98/accelerated-spark-gpu.git
cd accelerated-spark-gpu

# Run pipeline
sbt "core/run"
```

## Output

After running the pipeline, you'll find:

- `data/` - Generated datasets
- `models/` - Trained ML models
- `reports/` - Performance reports
- `sample-data/` - Sample CSV files

## Troubleshooting

- Ensure Java 11+ is installed
- Check that SBT is in your PATH
- For GPU acceleration:
    - Install NVIDIA drivers
    - Install CUDA Toolkit
    - Verify GPU compatibility with `nvidia-smi`
- Check for sufficient memory (8GB+ recommended)