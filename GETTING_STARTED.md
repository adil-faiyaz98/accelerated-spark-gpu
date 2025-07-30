# Getting Started

## Prerequisites

- Java 11+
- SBT (Scala Build Tool)
- 8GB+ RAM
- Git (for cloning the repository)
- NVIDIA GPU (optional, for GPU acceleration)

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

### Install Git

```bash
# Ubuntu/Debian
sudo apt install git

# macOS
brew install git

# Windows - download from git-scm.com
```

## Running
```bash
git clone https://github.com/adil-faiyaz98/accelerated-spark-gpu.git
cd accelerated-spark-gpu
sbt "core/run"
```

## Output

After running, you'll find:

- `data/` - Generated datasets
- `models/` - Trained ML models
- `reports/` - Performance reports

## Troubleshooting

- Ensure Java 11+ is installed
- Check SBT is in your PATH
- Verify Git is installed and accessible
- For GPU acceleration, install NVIDIA drivers and CUDA