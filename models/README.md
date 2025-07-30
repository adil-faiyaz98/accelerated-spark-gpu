# Models Directory

This directory contains pre-trained machine learning models and their associated metadata for the GPU-accelerated Spark pipeline.

## Purpose

The models directory serves as a repository for:
- Trained ML models in Spark MLlib format
- Model metadata and configuration information
- Performance metrics and validation results
- Model versioning and deployment artifacts

## Directory Structure

```
models/
├── linear_regression_model/           # Linear Regression Pipeline Model
│   ├── metadata.json                 # Main pipeline metadata
│   └── stages/                       # Pipeline stages
│       ├── 0_VectorAssembler/        # Feature assembly stage
│       │   └── metadata.json         # VectorAssembler configuration
│       ├── 1_StandardScaler/         # Feature scaling stage
│       │   └── metadata.json         # StandardScaler parameters
│       └── 2_LinearRegression/       # Linear regression stage
│           └── metadata.json         # Model coefficients and metrics
└── random_forest_model/              # Random Forest Pipeline Model
    ├── metadata.json                 # Main pipeline metadata
    └── stages/                       # Pipeline stages
        ├── 0_VectorAssembler/        # Feature assembly stage
        │   └── metadata.json         # VectorAssembler configuration
        ├── 1_StandardScaler/         # Feature scaling stage
        │   └── metadata.json         # StandardScaler parameters
        └── 2_RandomForestRegressor/  # Random forest stage
            └── metadata.json         # Tree parameters and feature importance
```

## Model Information

### Linear Regression Model
- **Purpose**: Housing price prediction using linear regression
- **Features**: 8 numerical features (rooms, bedrooms, bathrooms, square_feet, age, distance, crime_rate, school_rating)
- **Target**: Housing price prediction
- **Performance**: RMSE: 52,891.23, MAE: 38,742.15, R²: 0.7234

### Random Forest Model
- **Purpose**: Housing price prediction using ensemble methods
- **Features**: Same 8 numerical features as linear regression
- **Target**: Housing price prediction
- **Performance**: RMSE: 45,234.67, MAE: 32,156.89, R²: 0.7834
- **Configuration**: 100 trees, max depth 10, seed 42

## Model Metadata

### Pipeline Metadata (`metadata.json`)
Each model directory contains a main metadata file with:
- **Model Class**: Spark MLlib model class information
- **Timestamp**: Model training timestamp
- **Spark Version**: Compatible Spark version (3.5.1)
- **UID**: Unique identifier for the pipeline
- **Parameter Map**: Complete pipeline configuration
- **Performance Metrics**: Training and validation results

### Stage Metadata (`stages/*/metadata.json`)
Each pipeline stage has detailed metadata including:
- **Stage Configuration**: Parameters and settings
- **Input/Output Columns**: Data flow information
- **Model-Specific Data**: Coefficients, feature importance, scaling parameters

## Feature Information

### Input Features
1. **total_rooms** (Integer): Total number of rooms in the house
2. **bedrooms** (Integer): Number of bedrooms
3. **bathrooms** (Integer): Number of bathrooms
4. **square_feet** (Integer): Total square footage
5. **age_years** (Integer): Age of the house in years
6. **distance_to_center_km** (Double): Distance to city center in kilometers
7. **crime_rate** (Double): Local crime rate (0-10 scale)
8. **school_rating** (Integer): Local school rating (1-10 scale)

### Feature Engineering Pipeline
1. **VectorAssembler**: Combines all features into a single feature vector
2. **StandardScaler**: Normalizes features (mean=0, std=1) for better model performance
3. **Model Training**: Applies the specific algorithm (Linear Regression or Random Forest)

## Model Performance Comparison

| Model | RMSE | MAE | R² | Training Time | Best Use Case |
|-------|------|-----|----|--------------| -------------|
| Linear Regression | 52,891.23 | 38,742.15 | 0.7234 | 3.89s | Simple, interpretable predictions |
| Random Forest | 45,234.67 | 32,156.89 | 0.7834 | 12.45s | Higher accuracy, feature importance |

### Feature Importance (Random Forest)
1. **square_feet**: 0.342 (34.2%) - Most important feature
2. **school_rating**: 0.198 (19.8%) - Second most important
3. **total_rooms**: 0.156 (15.6%) - Third most important
4. **distance_to_center_km**: 0.134 (13.4%)
5. **crime_rate**: 0.089 (8.9%)
6. **age_years**: 0.045 (4.5%)
7. **bedrooms**: 0.023 (2.3%)
8. **bathrooms**: 0.013 (1.3%) - Least important

## Usage Examples

### Loading Models in Scala
```scala
import org.apache.spark.ml.PipelineModel

// Load Linear Regression model
val lrModel = PipelineModel.load("models/linear_regression_model")

// Load Random Forest model
val rfModel = PipelineModel.load("models/random_forest_model")

// Make predictions
val predictions = rfModel.transform(testData)
predictions.select("features", "prediction").show()
```

### Model Evaluation
```scala
import org.apache.spark.ml.evaluation.RegressionEvaluator

val evaluator = new RegressionEvaluator()
  .setLabelCol("price")
  .setPredictionCol("prediction")
  .setMetricName("rmse")

val rmse = evaluator.evaluate(predictions)
println(s"Root Mean Squared Error: $rmse")
```

### Feature Importance Analysis
```scala
// Extract Random Forest model from pipeline
val rfStage = rfModel.stages.last.asInstanceOf[RandomForestRegressionModel]

// Get feature importance
val featureImportances = rfStage.featureImportances.toArray
val featureNames = Array("total_rooms", "bedrooms", "bathrooms", "square_feet", 
                        "age_years", "distance_to_center_km", "crime_rate", "school_rating")

// Display feature importance
featureNames.zip(featureImportances).sortBy(-_._2).foreach { case (name, importance) =>
  println(f"$name: ${importance * 100}%.1f%%")
}
```

## Model Versioning

### Version Information
- **Current Version**: 1.0.0
- **Spark Compatibility**: 3.5.1
- **Training Date**: 2024-01-21
- **Training Dataset**: 400,000 records
- **Validation Dataset**: 100,000 records

### Model Updates
When updating models:
1. **Backup Previous Version**: Archive existing models with timestamp
2. **Update Metadata**: Ensure all metadata reflects new model parameters
3. **Performance Validation**: Compare new model performance against baseline
4. **Documentation**: Update this README with new performance metrics

## Deployment Considerations

### Production Deployment
- **Model Loading**: Use `PipelineModel.load()` for production deployment
- **Feature Consistency**: Ensure production features match training features
- **Performance Monitoring**: Monitor prediction accuracy and latency
- **Model Refresh**: Establish schedule for model retraining and updates

### Scaling Considerations
- **Memory Requirements**: Models require ~100MB memory when loaded
- **Prediction Latency**: ~1ms per prediction on standard hardware
- **Batch Processing**: Optimized for batch prediction on large datasets
- **GPU Acceleration**: Compatible with RAPIDS for GPU-accelerated inference

## Quality Assurance

### Model Validation
- **Cross-Validation**: 5-fold cross-validation during training
- **Hold-Out Testing**: 20% of data reserved for final validation
- **Statistical Tests**: Residual analysis and normality tests
- **Business Validation**: Results reviewed by domain experts

### Performance Monitoring
- **Accuracy Tracking**: Monitor prediction accuracy over time
- **Data Drift Detection**: Monitor for changes in input data distribution
- **Model Degradation**: Alert when performance drops below thresholds
- **Retraining Triggers**: Automatic retraining when accuracy degrades

## Troubleshooting

### Common Issues
- **Loading Errors**: Verify Spark version compatibility (3.5.1)
- **Feature Mismatch**: Ensure input data has all required features
- **Memory Issues**: Increase driver memory for large model loading
- **Performance Degradation**: Check for data drift or model staleness

### Debug Information
```scala
// Check model metadata
val metadata = spark.read.json("models/random_forest_model/metadata.json")
metadata.show(false)

// Validate feature schema
val expectedFeatures = Array("total_rooms", "bedrooms", "bathrooms", "square_feet", 
                            "age_years", "distance_to_center_km", "crime_rate", "school_rating")
val actualFeatures = testData.columns
assert(expectedFeatures.forall(actualFeatures.contains), "Missing required features")
```
