import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression._
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.Pipeline
import ml.dmlc.xgboost4j.scala.spark.XGBoostRegressor
import java.io.{File, PrintWriter}
import scala.util.{Random, Try, Success, Failure}

/**
 * GPU-Accelerated Spark Pipeline for machine learning workloads.
 * Demonstrates performance improvements with RAPIDS acceleration.
 */
object AcceleratedSparkPipeline {

  private val HOUSING_RECORDS = 500000
  private val TAXI_RECORDS = 1000000
  private val RANDOM_SEED = 42L

  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()

    try {
      runPipeline(spark)
    } catch {
      case e: Exception =>
        println(s"Pipeline failed: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
    }
  }

  private def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("GPU-Accelerated Spark Pipeline")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.adaptive.skewJoin.enabled", "true")
      .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .config("spark.rapids.sql.enabled", "true")
      .config("spark.rapids.memory.gpu.pooling.enabled", "true")
      .config("spark.rapids.sql.concurrentGpuTasks", "2")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.execution.arrow.pyspark.enabled", "true")
      .getOrCreate()
  }

  def runPipeline(spark: SparkSession): Unit = {
    println("=== GPU-Accelerated Spark Pipeline Started ===")

    // Step 1: Generate synthetic data
    println("Step 1: Generating synthetic datasets...")
    val housingData = generateHousingData(spark)
    val taxiData = generateTaxiData(spark)

    // Step 2: Data preprocessing and feature engineering
    println("Step 2: Preprocessing and feature engineering...")
    val processedHousing = preprocessHousingData(housingData)
    val processedTaxi = preprocessTaxiData(taxiData)

    // Step 3: Train machine learning models
    println("Step 3: Training ML models...")
    trainHousingPriceModel(processedHousing)
    trainTaxiFareModel(processedTaxi)

    // Step 4: Save datasets for benchmarking
    println("Step 4: Saving datasets...")
    saveDatasets(housingData, taxiData)

    println("=== Pipeline completed successfully ===")
  }

  def generateHousingData(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val random = new Random(RANDOM_SEED)
    println(s"Generating $HOUSING_RECORDS housing records...")

    val housingData = (1 to HOUSING_RECORDS).map { i =>
      val bedrooms = random.nextInt(5) + 1
      val bathrooms = random.nextInt(3) + 1
      val sqft = 500 + random.nextInt(2500)
      val age = random.nextInt(50)
      val lotSize = 1000 + random.nextInt(9000)
      val garage = random.nextInt(3)
      val neighborhood = s"neighborhood_${random.nextInt(50)}"
      val schoolRating = random.nextInt(10) + 1
      val crimeRate = random.nextDouble() * 10
      val distanceToCenter = random.nextDouble() * 30

      // Realistic price calculation with correlation
      val basePrice = 50000 + (sqft * 150) + (bedrooms * 10000) +
                     (bathrooms * 8000) - (age * 500) + (schoolRating * 5000) -
                     (crimeRate * 2000)
      val noise = random.nextGaussian() * 10000
      val price = math.max(30000, basePrice + noise)

      (i, bedrooms, bathrooms, sqft, age, lotSize, garage, neighborhood,
       schoolRating, crimeRate, distanceToCenter, price)
    }.toDF("id", "bedrooms", "bathrooms", "sqft", "age", "lot_size", "garage",
           "neighborhood", "school_rating", "crime_rate", "distance_to_center", "price")

    println(s"Generated ${housingData.count()} housing records")
    housingData.cache()
  }

  def generateTaxiData(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val random = new Random(RANDOM_SEED)
    println(s"Generating $TAXI_RECORDS taxi trip records...")

    val taxiData = (1 to TAXI_RECORDS).map { i =>
      val pickupLat = 40.7128 + (random.nextGaussian() * 0.1)
      val pickupLon = -74.0060 + (random.nextGaussian() * 0.1)
      val dropoffLat = 40.7128 + (random.nextGaussian() * 0.1)
      val dropoffLon = -74.0060 + (random.nextGaussian() * 0.1)

      val distance = math.sqrt(
        math.pow(pickupLat - dropoffLat, 2) +
        math.pow(pickupLon - dropoffLon, 2)
      ) * 111000 // Convert to meters

      val duration = 300 + random.nextInt(1800) // 5-35 minutes
      val passengers = random.nextInt(4) + 1
      val hour = random.nextInt(24)
      val dayOfWeek = random.nextInt(7) + 1

      // Realistic fare calculation
      val baseFare = 2.50 + (distance * 0.002) + (duration * 0.01)
      val surge = if ((hour >= 7 && hour <= 9) || (hour >= 17 && hour <= 19)) 1.5 else 1.0
      val fare = baseFare * surge
      val tip = if (random.nextDouble() > 0.3) {
        fare * (0.1 + random.nextDouble() * 0.2)
      } else 0.0

      (i, pickupLat, pickupLon, dropoffLat, dropoffLon, distance, duration,
       passengers, hour, dayOfWeek, fare, tip)
    }.toDF("trip_id", "pickup_lat", "pickup_lon", "dropoff_lat", "dropoff_lon",
           "distance", "duration", "passengers", "hour", "day_of_week", "fare", "tip")

    println(s"Generated ${taxiData.count()} taxi trip records")
    taxiData.cache()
  }

  def preprocessHousingData(data: DataFrame): DataFrame = {
    data
      .filter(col("price") > 30000 && col("price") < 2000000)
      .filter(col("sqft") > 300 && col("sqft") < 5000)
      .withColumn("price_per_sqft", col("price") / col("sqft"))
      .withColumn("total_rooms", col("bedrooms") + col("bathrooms"))
      .withColumn("age_category",
        when(col("age") < 5, "new")
          .when(col("age") < 15, "recent")
          .when(col("age") < 30, "established")
          .otherwise("old"))
  }

  def preprocessTaxiData(data: DataFrame): DataFrame = {
    data
      .filter(col("fare") > 0 && col("fare") < 200)
      .filter(col("distance") > 0 && col("distance") < 50000)
      .filter(col("duration") > 60 && col("duration") < 7200)
      .withColumn("speed", col("distance") / col("duration") * 3.6)
      .withColumn("fare_per_mile", col("fare") / (col("distance") / 1609.34))
      .withColumn("tip_percentage", col("tip") / col("fare") * 100)
      .withColumn("time_category",
        when(col("hour") >= 6 && col("hour") < 12, "morning")
          .when(col("hour") >= 12 && col("hour") < 18, "afternoon")
          .when(col("hour") >= 18 && col("hour") < 24, "evening")
          .otherwise("night"))
  }

  def trainHousingPriceModel(data: DataFrame): Unit = {
    println("Training housing price prediction model...")

    val featureCols = Array("bedrooms", "bathrooms", "sqft", "age", "lot_size",
                           "garage", "school_rating", "crime_rate", "distance_to_center")

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaled_features")
      .setWithStd(true)
      .setWithMean(true)

    val rf = new RandomForestRegressor()
      .setLabelCol("price")
      .setFeaturesCol("scaled_features")
      .setNumTrees(100)
      .setMaxDepth(10)
      .setSeed(RANDOM_SEED)

    val xgb = new XGBoostRegressor()
      .setLabelCol("price")
      .setFeaturesCol("scaled_features")
      .setMaxDepth(6)
      .setEta(0.1)
      .setNumRound(100)
      .setObjective("reg:squarederror")

    val Array(trainData, testData) = data.randomSplit(Array(0.8, 0.2), seed = RANDOM_SEED)

    // Train Random Forest
    val rfPipeline = new Pipeline().setStages(Array(assembler, scaler, rf))
    val rfModel = rfPipeline.fit(trainData)
    val rfPredictions = rfModel.transform(testData)

    // Train XGBoost
    val xgbPipeline = new Pipeline().setStages(Array(assembler, scaler, xgb))
    val xgbModel = xgbPipeline.fit(trainData)
    val xgbPredictions = xgbModel.transform(testData)

    // Evaluate models
    val evaluator = new RegressionEvaluator()
      .setLabelCol("price")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val rfRmse = evaluator.evaluate(rfPredictions)
    val xgbRmse = evaluator.evaluate(xgbPredictions)

    println(f"Random Forest RMSE: $rfRmse%.2f")
    println(f"XGBoost RMSE: $xgbRmse%.2f")

    // Save models
    ensureDirectoryExists("models")
    rfModel.write.overwrite().save("models/rf_model")
    xgbModel.write.overwrite().save("models/xgb_model")

    println("Models saved successfully")
  }

  def trainTaxiFareModel(data: DataFrame): Unit = {
    println("Training taxi fare prediction model...")

    val featureCols = Array("distance", "duration", "passengers", "hour", "day_of_week")

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaled_features")
      .setWithStd(true)
      .setWithMean(true)

    val lr = new LinearRegression()
      .setLabelCol("fare")
      .setFeaturesCol("scaled_features")
      .setMaxIter(100)
      .setRegParam(0.01)

    val Array(trainData, testData) = data.randomSplit(Array(0.8, 0.2), seed = RANDOM_SEED)

    val pipeline = new Pipeline().setStages(Array(assembler, scaler, lr))
    val model = pipeline.fit(trainData)
    val predictions = model.transform(testData)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("fare")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val rmse = evaluator.evaluate(predictions)
    println(f"Taxi Fare Model RMSE: $rmse%.2f")

    // Save model
    model.write.overwrite().save("models/taxi_fare_model")
    println("Taxi fare model saved successfully")
  }

  def saveDatasets(housingData: DataFrame, taxiData: DataFrame): Unit = {
    ensureDirectoryExists("data")
    ensureDirectoryExists("sample-data")

    println("Saving datasets...")

    // Save full datasets
    housingData.coalesce(1).write
      .mode("overwrite")
      .option("header", "true")
      .csv("data/housing.csv")

    taxiData.coalesce(1).write
      .mode("overwrite")
      .option("header", "true")
      .csv("data/taxi.csv")

    // Save sample data for inspection
    housingData.sample(0.01).coalesce(1).write
      .mode("overwrite")
      .option("header", "true")
      .csv("sample-data/sample_housing.csv")

    taxiData.sample(0.001).coalesce(1).write
      .mode("overwrite")
      .option("header", "true")
      .csv("sample-data/sample_taxi.csv")

    println("Datasets saved successfully")
  }

  private def ensureDirectoryExists(path: String): Unit = {
    val dir = new File(path)
    if (!dir.exists()) {
      dir.mkdirs()
    }
  }
}