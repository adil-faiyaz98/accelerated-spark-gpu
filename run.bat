@echo off
echo ================================================================================
echo ENTERPRISE GPU-ACCELERATED SPARK PIPELINE
echo Single-Command Execution System
echo ================================================================================ REM Check if Java is installed
java -version >nul 2>&1
if %errorlevel% neq 0 ( echo ERROR: Java is not installed or not in PATH echo Please install Java 11 or later echo Download from: https://adoptium.net/ pause exit /b 1
) REM Check if SBT is installed
sbt --version >nul 2>&1
if %errorlevel% neq 0 ( echo ERROR: SBT is not installed or not in PATH echo Please install SBT from: https://www.scala-sbt.org/download.html pause exit /b 1
) REM Create required directories
if not exist data mkdir data
if not exist models mkdir models
if not exist reports mkdir reports
if not exist logs mkdir logs echo System requirements validated successfully!
echo. REM Parse command line arguments
set "ARGS="
if "%1"=="--quick" set "ARGS=--quick"
if "%1"=="--benchmark-only" set "ARGS=--benchmark-only"
if "%1"=="--no-gpu" set "ARGS=--no-gpu"
if "%1"=="--help" set "ARGS=--help" if "%ARGS%"=="" ( if "%1"=="" ( echo Starting complete GPU-accelerated pipeline workflow... echo This will execute: echo 1. Machine Learning Pipeline with synthetic data generation echo 2. Performance Benchmarking ^(CPU vs GPU comparison^) echo 3. Professional Report Generation with visualizations echo. echo For quick demo, use: run.bat --quick echo For help, use: run.bat --help echo. pause set "ARGS=" ) else ( set "ARGS=%1" )
) echo Executing Enterprise GPU Pipeline...
echo Arguments: %ARGS%
echo. REM Execute the pipeline
if "%ARGS%"=="" ( sbt "core/run"
) else ( sbt "core/run %ARGS%"
) if %errorlevel% neq 0 ( echo. echo ================================================================================ echo EXECUTION FAILED echo ================================================================================ echo Check the logs above for error details. echo Common issues: echo - Insufficient memory ^(try increasing JVM heap with -Xmx4g^) echo - Missing dependencies ^(run: sbt update^) echo - GPU driver issues ^(GPU acceleration will fall back to CPU^) echo. pause exit /b 1
) echo.
echo ================================================================================
echo EXECUTION COMPLETED SUCCESSFULLY!
echo ================================================================================
echo.
echo Results are available in the reports/ directory:
echo - HTML Report: Open reports/gpu_acceleration_report_*.html in your browser
echo - Performance Charts: View PNG files in reports/
echo - CSV Data: Available for further analysis
echo - Executive Summary: Business-focused results in reports/
echo.
echo Next steps:
echo 1. Open the HTML report to view comprehensive results
echo 2. Share performance charts with stakeholders
echo 3. Consider scaling to larger datasets for production evaluation
echo.
pause