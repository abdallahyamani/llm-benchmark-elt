import logging
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def get_or_create_spark() -> SparkSession:
    """Create or retrieve a SparkSession configured for Delta Lake."""
    try:

        builder = (
            SparkSession.builder.appName("LLM-Benchmark-Silver")
            .master("local[*]")
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.hadoop.fs.permissions.umask-mode", "000")
        )

        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        logger.info("SparkSession created successfully.")
        return spark

    except Exception as exc:
        raise RuntimeError(
            f"Failed to create SparkSession: {exc}"
        ) from exc
