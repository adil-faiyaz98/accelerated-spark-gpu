import scala.util.{Try, Success, Failure}

/**
 * Main entry point for the GPU-Accelerated Spark Pipeline application.
 * Provides a unified interface to run different components of the pipeline.
 */
object Main {

  private val USAGE_MESSAGE =
    """Usage: sbt 'run [pipeline|benchmark|evaluate|all]'
      |  pipeline  - Run the main ML pipeline
      |  benchmark - Run performance benchmarks
      |  evaluate  - Evaluate trained models
      |  all       - Run all components sequentially
      |""".stripMargin

  def main(args: Array[String]): Unit = {
    args.headOption match {
      case Some("pipeline") =>
        executeComponent("Pipeline", () => AcceleratedSparkPipeline.main(args.tail))

      case Some("benchmark") =>
        executeComponent("Benchmark", () => GPUBenchmark.main(args.tail))

      case Some("evaluate") =>
        executeComponent("Model Evaluation", () => ModelEvaluator.main(args.tail))

      case Some("all") =>
        executeAllComponents()

      case _ =>
        println(USAGE_MESSAGE)
        System.exit(1)
    }
  }

  private def executeComponent(componentName: String, component: () => Unit): Unit = {
    println(s"=== Starting $componentName ===")
    Try(component()) match {
      case Success(_) =>
        println(s"=== $componentName completed successfully ===")
      case Failure(exception) =>
        println(s"=== $componentName failed: ${exception.getMessage} ===")
        exception.printStackTrace()
        System.exit(1)
    }
  }

  private def executeAllComponents(): Unit = {
    println("=== Running Complete GPU-Accelerated Pipeline ===")

    val components = List(
      ("Pipeline", () => AcceleratedSparkPipeline.main(Array())),
      ("Benchmark", () => GPUBenchmark.main(Array())),
      ("Model Evaluation", () => ModelEvaluator.main(Array()))
    )

    components.foreach { case (name, component) =>
      executeComponent(name, component)
    }

    println("=== All components completed successfully ===")
  }
}