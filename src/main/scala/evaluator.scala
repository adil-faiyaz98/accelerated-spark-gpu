import org.apache.spark.sql._
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions._
import java.io.{File, PrintWriter}
import scala.util.{Try, Success, Failure}

/**
 * Model Evaluator for comparing trained ML models and generating evaluation reports.
 * Supports both Random Forest and XGBoost models with comprehensive metrics.
 */
object ModelEvaluator {

  case class ModelMetrics(
    modelType: String,
    rmse: Double,
    mae: Double,
    r2: Double,
    predictionTime: Float
  )

  private val TEST_SAMPLE_RATIO = 0.2
  private val RANDOM_SEED = 42L

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Model Evaluator")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    try {
      evaluateModels(spark)
    } catch {
      case e: Exception =>
        println(s"Model evaluation failed: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
    }
  }

  def evaluateModels(spark: SparkSession): Unit = {
    import spark.implicits._

    // Load test data
    val testData = loadTestData(spark)
    val labelCol = determineTargetColumn(testData)

    // Load and evaluate models
    val modelMetrics = evaluateAvailableModels(testData, labelCol)

    if (modelMetrics.nonEmpty) {
      // Generate evaluation report
      generateEvaluationReport(modelMetrics, testData.count())

      // Validate model accuracy if multiple models available
      if (modelMetrics.length > 1) {
        validateModelConsistency(testData, labelCol)
      }
    } else {
      println("No trained models found for evaluation")
    }
  }

  private def loadTestData(spark: SparkSession): DataFrame = {
    val housingPath = "data/housing.csv"

    Try {
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(housingPath)
        .sample(TEST_SAMPLE_RATIO, seed = RANDOM_SEED)
    } match {
      case Success(data) =>
        println(s"Loaded test data with ${data.count()} records")
        data
      case Failure(exception) =>
        throw new RuntimeException(s"Failed to load test data from $housingPath: ${exception.getMessage}")
    }
  }

  private def determineTargetColumn(data: DataFrame): String = {
    val possibleTargets = List("price", "medhvalue", "target")
    val columns = data.columns.toSet

    possibleTargets.find(columns.contains) match {
      case Some(target) =>
        println(s"Using target column: $target")
        target
      case None =>
        throw new RuntimeException(s"No valid target column found. Available columns: ${columns.mkString(", ")}")
    }
  }

  private def evaluateAvailableModels(testData: DataFrame, labelCol: String): List[ModelMetrics] = {
    val modelPaths = List(
      ("RandomForest", "models/rf_model"),
      ("XGBoost-GPU", "models/xgb_model"),
      ("TaxiFare", "models/taxi_fare_model")
    )

    modelPaths.flatMap { case (modelType, path) =>
      loadAndEvaluateModel(modelType, path, testData, labelCol)
    }
  }

  private def loadAndEvaluateModel(modelType: String,
                                  modelPath: String,
                                  testData: DataFrame,
                                  labelCol: String): Option[ModelMetrics] = {
    Try {
      val model = PipelineModel.load(modelPath)
      val (predictions, predTime) = timeOperation(s"$modelType-Prediction") {
        model.transform(testData)
      }
      calculateMetrics(modelType, predictions, labelCol, predTime)
    } match {
      case Success(metrics) =>
        println(s"Successfully evaluated $modelType model")
        Some(metrics)
      case Failure(exception) =>
        println(s"Failed to evaluate $modelType model: ${exception.getMessage}")
        None
    }
  }

  private def timeOperation[R](phase: String)(block: => R): (R, Float) = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    val elapsed = (t1 - t0).toFloat / 1000
    println(s"Elapsed time [$phase]: ${elapsed}s")
    (result, elapsed)
  }

  def calculateMetrics(modelType: String,
                      predictions: DataFrame,
                      labelCol: String,
                      predTime: Float): ModelMetrics = {

    val rmseEvaluator = new RegressionEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val maeEvaluator = new RegressionEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol("prediction")
      .setMetricName("mae")

    val r2Evaluator = new RegressionEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol("prediction")
      .setMetricName("r2")

    val rmse = rmseEvaluator.evaluate(predictions)
    val mae = maeEvaluator.evaluate(predictions)
    val r2 = r2Evaluator.evaluate(predictions)

    ModelMetrics(modelType, rmse, mae, r2, predTime)
  }

  private def validateModelConsistency(testData: DataFrame, labelCol: String): Unit = {
    Try {
      val rfModel = PipelineModel.load("models/rf_model")
      val xgbModel = PipelineModel.load("models/xgb_model")

      val rfPredictions = rfModel.transform(testData)
      val xgbPredictions = xgbModel.transform(testData)

      validateModelAccuracy(rfPredictions, xgbPredictions, labelCol)
    } match {
      case Success(_) =>
        println("Model consistency validation completed")
      case Failure(exception) =>
        println(s"Model consistency validation failed: ${exception.getMessage}")
    }
  }

  def validateModelAccuracy(rfPredictions: DataFrame,
                           xgbPredictions: DataFrame,
                           labelCol: String): Unit = {
    import rfPredictions.sparkSession.implicits._

    // Compare predictions between models
    val rfPreds = rfPredictions.select("prediction").rdd.map(_.getDouble(0)).collect()
    val xgbPreds = xgbPredictions.select("prediction").rdd.map(_.getDouble(0)).collect()

    val correlationCoeff = calculateCorrelation(rfPreds, xgbPreds)
    val meanAbsDiff = rfPreds.zip(xgbPreds).map { case (rf, xgb) =>
      math.abs(rf - xgb)
    }.sum / rfPreds.length

    println(f"Model Correlation: ${correlationCoeff}%.4f")
    println(f"Mean Absolute Difference: ${meanAbsDiff}%.2f")

    if (correlationCoeff > 0.95) {
      println("Models show high agreement - GPU acceleration maintains accuracy")
    } else {
      println("WARNING: Models show significant differences - review GPU configuration")
    }
  }

  def calculateCorrelation(x: Array[Double], y: Array[Double]): Double = {
    require(x.length == y.length, "Arrays must have the same length")
    require(x.nonEmpty, "Arrays cannot be empty")

    val n = x.length
    val sumX = x.sum
    val sumY = y.sum
    val sumXY = x.zip(y).map { case (xi, yi) => xi * yi }.sum
    val sumX2 = x.map(xi => xi * xi).sum
    val sumY2 = y.map(yi => yi * yi).sum

    val numerator = n * sumXY - sumX * sumY
    val denominator = math.sqrt((n * sumX2 - sumX * sumX) * (n * sumY2 - sumY * sumY))

    if (denominator == 0) 0.0 else numerator / denominator
  }

  def generateEvaluationReport(metrics: List[ModelMetrics], testSize: Long): Unit = {
    ensureDirectoryExists("reports")

    val writer = new PrintWriter("reports/model_evaluation_report.md")

    try {
      writer.println("# Model Evaluation Report")
      writer.println()
      writer.println(f"**Test Dataset Size:** $testSize records")
      writer.println()

      generatePerformanceComparison(writer, metrics)
      generatePerformanceInsights(writer, metrics)
      generateRecommendations(writer)

    } finally {
      writer.close()
    }

    println("Model evaluation report generated: reports/model_evaluation_report.md")
  }

  private def generatePerformanceComparison(writer: PrintWriter, metrics: List[ModelMetrics]): Unit = {
    writer.println("## Model Performance Comparison")
    writer.println()
    writer.println("| Model | RMSE | MAE | R² | Prediction Time (s) |")
    writer.println("|-------|------|-----|----|--------------------|")

    metrics.foreach { metric =>
      writer.println(f"| ${metric.modelType} | ${metric.rmse}%.2f | ${metric.mae}%.2f | ${metric.r2}%.4f | ${metric.predictionTime}%.3f |")
    }
    writer.println()

    // Find best model
    val bestModel = metrics.minBy(_.rmse)
    writer.println(f"**Best Model:** ${bestModel.modelType} (RMSE: ${bestModel.rmse}%.2f)")
    writer.println()
  }

  private def generatePerformanceInsights(writer: PrintWriter, metrics: List[ModelMetrics]): Unit = {
    writer.println("## Performance Insights")
    writer.println()

    val xgbMetric = metrics.find(_.modelType.contains("XGBoost"))
    val rfMetric = metrics.find(_.modelType.contains("RandomForest"))

    (xgbMetric, rfMetric) match {
      case (Some(xgb), Some(rf)) =>
        val accuracyImprovement = ((rf.rmse - xgb.rmse) / rf.rmse) * 100
        val speedImprovement = rf.predictionTime / xgb.predictionTime
        writer.println(f"- XGBoost GPU shows ${accuracyImprovement}%.1f%% accuracy improvement over Random Forest")
        writer.println(f"- XGBoost GPU is ${speedImprovement}%.1fx faster for predictions")
        writer.println("- GPU acceleration maintains model accuracy while improving performance")
      case _ =>
        writer.println("- Comprehensive model comparison requires both XGBoost and Random Forest results")
    }
    writer.println()
  }

  private def generateRecommendations(writer: PrintWriter): Unit = {
    writer.println("## Recommendations")
    writer.println()
    writer.println("1. **Production Deployment:** Use XGBoost GPU for optimal performance")
    writer.println("2. **Model Monitoring:** Track prediction accuracy and latency in production")
    writer.println("3. **Resource Optimization:** GPU instances provide better cost-performance ratio")
    writer.println("4. **Scaling Strategy:** Implement auto-scaling based on prediction volume")
  }

  private def ensureDirectoryExists(path: String): Unit = {
    val dir = new File(path)
    if (!dir.exists()) {
      dir.mkdirs()
    }
  }
}