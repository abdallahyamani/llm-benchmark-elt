from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

MODEL_LEADERBOARD_SCHEMA = StructType([
    StructField("snapshot_date", DateType(), False),
    StructField("rank", IntegerType(), False),
    StructField("model_id", StringType(), False),
    StructField("model_name", StringType(), False),
    StructField("vendor", StringType(), True),
    StructField("composite_score", DoubleType(), False),
    StructField("intelligence_norm", DoubleType(), True),
    StructField("speed_norm", DoubleType(), True),
    StructField("price_norm", DoubleType(), True),
    StructField("avg_price_per_1m_tokens", DoubleType(), True),
    StructField("intelligence_per_dollar", DoubleType(), True),
    StructField("cost_tier", StringType(), True),
    StructField("efficiency_rank", IntegerType(), True),
])

MODEL_TRENDS_SCHEMA = StructType([
    StructField("model_id", StringType(), False),
    StructField("model_name", StringType(), False),
    StructField("vendor", StringType(), True),
    StructField("snapshot_date", DateType(), False),
    StructField("prev_snapshot_date", DateType(), True),
    StructField("intelligence_current", DoubleType(), True),
    StructField("intelligence_previous", DoubleType(), True),
    StructField("intelligence_delta", DoubleType(), True),
    StructField("speed_current", DoubleType(), True),
    StructField("speed_previous", DoubleType(), True),
    StructField("speed_delta", DoubleType(), True),
    StructField("price_current", DoubleType(), True),
    StructField("price_previous", DoubleType(), True),
    StructField("price_delta", DoubleType(), True),
])
