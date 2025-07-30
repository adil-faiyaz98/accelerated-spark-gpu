# Reporting Module

The reporting module generates comprehensive, professional reports for GPU acceleration analysis, including performance comparisons, visualizations, and executive summaries.

## Purpose

This module transforms benchmark results into actionable insights by:
- Creating professional HTML reports with embedded charts
- Generating executive summaries for stakeholders
- Producing CSV data files for further analysis
- Visualizing performance improvements and cost analysis

## Main Classes

### Report Generation
- **`ReportGenerationService`** - Core report generation orchestration
  - HTML report creation with embedded styling
  - CSV data export for analysis
  - Chart generation using JFreeChart
  - Executive summary generation

### Configuration
- **`ReportConfig`** - Configuration for report generation
  - Output directory specification
  - Chart generation settings
  - Report content options
  - Visualization parameters

### Result Data Classes
- **`ReportGenerationSummary`** - Summary of generated report files
  - File paths and generation metrics
  - Timestamp and metadata information

## Key Features

### Professional HTML Reports
- **Responsive Design**: Mobile-friendly HTML with CSS styling
- **Interactive Elements**: Embedded charts and performance metrics
- **Executive Summary**: Business-focused performance highlights
- **Technical Details**: Comprehensive configuration and results

### Data Export Capabilities
- **CSV Files**: Raw benchmark data for custom analysis
- **Chart Images**: PNG charts for presentations
- **Markdown Reports**: Executive summaries in Markdown format

### Visualization Support
- **Performance Charts**: CPU vs GPU comparison charts
- **Trend Analysis**: Performance scaling across dataset sizes
- **Cost Analysis**: ROI calculations and infrastructure comparisons

## Integration with Pipeline

The reporting module integrates with:

1. **Benchmarking Module** → Consumes benchmark results for analysis
2. **Core Module** → Uses configuration and utilities
3. **External Tools** → Exports data for BI tools and presentations

## Usage Examples

### Basic Report Generation
```scala
import com.enterprise.spark.gpu.reporting.ReportGenerationService

val reportService = new ReportGenerationService()
val config = ReportGenerationService.ReportConfig(
  outputDirectory = "reports",
  generateCharts = true,
  includeExecutiveSummary = true
)

// Generate reports from benchmark results
val reportSummary = reportService.generateComprehensiveReports(
  Some(benchmarkResults),
  config
)

println(s"Generated ${reportSummary.generatedFiles.length} report files")
println(s"Main report: ${reportSummary.htmlReportPath}")
```

### Custom Report Configuration
```scala
// Minimal report for quick analysis
val quickConfig = ReportConfig(
  outputDirectory = "quick-reports",
  generateCharts = false,
  includeExecutiveSummary = false,
  includeTechnicalDetails = false
)

// Comprehensive report for stakeholders
val fullConfig = ReportConfig(
  outputDirectory = "stakeholder-reports",
  generateCharts = true,
  includeExecutiveSummary = true,
  includeTechnicalDetails = true,
  chartWidth = 1200,
  chartHeight = 800
)
```

### Accessing Generated Reports
```scala
val reportSummary = reportService.generateComprehensiveReports(
  Some(benchmarkResults),
  config
)

// Open main HTML report
val htmlReport = reportSummary.htmlReportPath
println(s"Open in browser: file://${new java.io.File(htmlReport).getAbsolutePath}")

// Access CSV data files
val csvFiles = reportSummary.generatedFiles.filter(_.endsWith(".csv"))
csvFiles.foreach { csvFile =>
  println(s"Data file: $csvFile")
}
```

## Report Components

### Executive Summary
- **Key Performance Highlights**: GPU acceleration status and metrics
- **Business Impact**: Processing time improvements and cost savings
- **ROI Analysis**: Infrastructure cost comparisons and recommendations

### Performance Analysis
- **Benchmark Results**: Detailed CPU vs GPU performance comparisons
- **Speedup Metrics**: Quantified performance improvements
- **Scaling Analysis**: Performance across different dataset sizes

### Technical Details
- **System Configuration**: Hardware and software specifications
- **RAPIDS Settings**: GPU acceleration configuration details
- **Model Performance**: ML model accuracy and training metrics

### Visualizations
- **Performance Charts**: Bar charts comparing CPU vs GPU execution times
- **Trend Graphs**: Performance scaling with dataset size
- **Cost Analysis**: Infrastructure cost comparison charts

## Configuration Options

### Report Settings
```scala
case class ReportConfig(
  outputDirectory: String = "reports",        // Output directory for reports
  generateCharts: Boolean = true,             // Enable chart generation
  includeExecutiveSummary: Boolean = true,    // Include executive summary
  includeTechnicalDetails: Boolean = true,    // Include technical details
  chartWidth: Int = 800,                      // Chart width in pixels
  chartHeight: Int = 600                      // Chart height in pixels
)
```

### Chart Customization
```scala
// Custom chart dimensions for presentations
val presentationConfig = ReportConfig(
  chartWidth = 1920,   // Full HD width
  chartHeight = 1080,  // Full HD height
  generateCharts = true
)

// High-resolution charts for publications
val publicationConfig = ReportConfig(
  chartWidth = 3840,   // 4K width
  chartHeight = 2160,  // 4K height
  generateCharts = true
)
```

## Generated Files

### HTML Report
- **Main Report**: Comprehensive HTML report with embedded CSS
- **Interactive Elements**: Clickable charts and performance metrics
- **Professional Styling**: Corporate-ready design and layout

### Data Files
- **Benchmark CSV**: Raw benchmark results in CSV format
- **Model Metrics CSV**: ML model performance metrics
- **Summary CSV**: Aggregated performance statistics

### Charts and Visualizations
- **Performance Comparison**: CPU vs GPU execution time charts
- **Speedup Analysis**: Performance improvement visualizations
- **Cost Analysis**: Infrastructure cost comparison charts

### Executive Documents
- **Executive Summary**: Markdown format for easy sharing
- **Technical Brief**: Detailed technical analysis document

## Best Practices

1. **Audience-Appropriate**: Tailor content to technical vs business audiences
2. **Visual Clarity**: Use clear charts and consistent formatting
3. **Data Accuracy**: Ensure all metrics are correctly calculated and presented
4. **Professional Appearance**: Maintain corporate standards for external sharing

## Dependencies

- JFreeChart 1.5.3 (for chart generation)
- Apache Commons Math 3.6.1 (for statistical calculations)
- Scala CSV 1.3.10 (for data export)
- Core and Benchmarking modules

## Testing

Run reporting tests with:
```bash
sbt reporting/test
```

## Output Structure
```
reports/
├── gpu_acceleration_report_TIMESTAMP.html    # Main HTML report
├── model_comparison_TIMESTAMP.png            # Model performance chart
├── benchmark_comparison_TIMESTAMP.png        # CPU vs GPU chart
├── model_metrics_TIMESTAMP.csv               # Model performance data
├── benchmark_results_TIMESTAMP.csv           # Benchmark data
└── executive_summary_TIMESTAMP.md            # Executive summary
```