# Model Information

## Random Forest Model

- **RMSE**: 45,234
- **R² Score**: 0.78
- **Training Time**: 12s
- **Location**: models/random_forest_model/

## Linear Regression Model

- **RMSE**: 52,891
- **R² Score**: 0.72
- **Training Time**: 4s
- **Location**: models/linear_regression_model/

## Feature Importance
1. **square_feet**: 34.2%
2. **school_rating**: 19.8%
3. **total_rooms**: 15.6%
4. **distance_to_center**: 13.4%
5. **crime_rate**: 8.9%

## Usage

```scala
import org.apache.spark.ml.PipelineModel

val model = PipelineModel.load("models/random_forest_model")
val predictions = model.transform(newData)
```
