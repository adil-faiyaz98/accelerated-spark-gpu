FROM nvidia/cuda:11.8-runtime-ubuntu20.04

# Install Java 11
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk wget curl && \
    rm -rf /var/lib/apt/lists/*

# Install Scala and SBT
RUN wget https://github.com/sbt/sbt/releases/download/v1.9.6/sbt-1.9.6.tgz && \
    tar -xzf sbt-1.9.6.tgz && \
    mv sbt /opt/ && \
    ln -s /opt/sbt/bin/sbt /usr/local/bin/sbt

# Install Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.1-bin-hadoop3.tgz && \
    mv spark-3.5.1-bin-hadoop3 /opt/spark

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Copy project files
WORKDIR /opt/spark
COPY . .

# Build the project
RUN sbt compile "core/compile"

# Expose Spark UI port
EXPOSE 4040

# Default command
CMD ["sbt", "core/run"]