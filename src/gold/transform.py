import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

COMPOSITE_WEIGHTS = {
    "intelligence": 0.50,
    "speed": 0.30,
    "price": 0.20,
}

COST_TIER_THRESHOLDS = {
    "budget": 0.33,
    "mid": 0.66,
    "premium": 1.00,
}


def _min_max_normalize(df: DataFrame, col_name: str, output_col: str) -> DataFrame:
    """Apply min-max normalization scaling a column to [0, 100] with 50.0 fallback."""
    stats = df.agg(
        F.min(col_name).alias("col_min"),
        F.max(col_name).alias("col_max"),
    ).first()

    col_min = stats["col_min"]
    col_max = stats["col_max"]

    if col_min is None or col_max is None or col_min == col_max:
        return df.withColumn(output_col, F.lit(50.0))

    return df.withColumn(
        output_col,
        ((F.col(col_name) - F.lit(col_min)) / (F.lit(col_max) - F.lit(col_min))) * 100.0,
    )


def _filter_scorable_models(df: DataFrame, snapshot_date: str) -> DataFrame:
    """Filter to snapshot and keep models that have every scoring metric."""
    return df.filter(F.col("snapshot_date") == snapshot_date).filter(
        F.col("analysis_ai_index").isNotNull()
        & F.col("output_tokens_per_sec").isNotNull()
        & F.col("input_1m_price").isNotNull()
        & F.col("output_1m_price").isNotNull()
        & (((F.col("input_1m_price") + F.col("output_1m_price")) / 2.0) > 0)
    )


def _compute_normalized_scores(df: DataFrame) -> DataFrame:
    """Normalize intelligence, speed, and price to [0, 100] and compute composite score."""
    df = df.withColumn(
        "avg_price",
        (F.col("input_1m_price") + F.col("output_1m_price")) / 2.0,
    )

    df = _min_max_normalize(df, "analysis_ai_index", "intelligence_norm")
    df = _min_max_normalize(df, "output_tokens_per_sec", "speed_norm")

    # Price inverted: lower price = higher score
    price_stats = df.agg(
        F.min("avg_price").alias("price_min"),
        F.max("avg_price").alias("price_max"),
    ).first()

    price_min = price_stats["price_min"]
    price_max = price_stats["price_max"]

    if price_min is not None and price_max is not None and price_min != price_max:
        df = df.withColumn(
            "price_norm",
            (1.0 - ((F.col("avg_price") - F.lit(price_min)) / (F.lit(price_max) - F.lit(price_min)))) * 100.0,
        )
    else:
        df = df.withColumn("price_norm", F.lit(50.0))

    return df.withColumn(
        "composite_score",
        F.col("intelligence_norm") * COMPOSITE_WEIGHTS["intelligence"]
        + F.col("speed_norm") * COMPOSITE_WEIGHTS["speed"]
        + F.col("price_norm") * COMPOSITE_WEIGHTS["price"],
    )


def _assign_composite_rank(df: DataFrame) -> DataFrame:
    """Assign rank 1..N by composite_score descending."""
    rank_window = Window.orderBy(F.col("composite_score").desc())
    return df.withColumn("rank", F.row_number().over(rank_window))


def _compute_cost_efficiency(df: DataFrame) -> DataFrame:
    """Add cost metrics: price per 1M tokens, intelligence per dollar, tier, and efficiency rank."""
    df = df.withColumnRenamed("avg_price", "avg_price_per_1m_tokens")
    df = df.withColumn(
        "intelligence_per_dollar",
        F.col("analysis_ai_index") / F.col("avg_price_per_1m_tokens"),
    )

    budget_upper, mid_upper = df.approxQuantile(
        "avg_price_per_1m_tokens",
        [COST_TIER_THRESHOLDS["budget"], COST_TIER_THRESHOLDS["mid"]],
        0.01,
    )
    df = df.withColumn(
        "cost_tier",
        F.when(F.col("avg_price_per_1m_tokens") <= budget_upper, F.lit("budget"))
        .when(F.col("avg_price_per_1m_tokens") <= mid_upper, F.lit("mid"))
        .otherwise(F.lit("premium")),
    )

    eff_window = Window.orderBy(F.col("intelligence_per_dollar").desc())
    return df.withColumn("efficiency_rank", F.row_number().over(eff_window))


def _select_ranking_columns(df: DataFrame) -> DataFrame:
    """Select final output columns matching MODEL_LEADERBOARD_SCHEMA."""
    return df.select(
        "snapshot_date",
        F.col("rank").cast("int").alias("rank"),
        "model_id",
        "model_name",
        "vendor",
        "composite_score",
        "intelligence_norm",
        "speed_norm",
        "price_norm",
        "avg_price_per_1m_tokens",
        "intelligence_per_dollar",
        "cost_tier",
        F.col("efficiency_rank").cast("int").alias("efficiency_rank"),
    )


def compute_model_rankings(df: DataFrame, snapshot_date: str) -> DataFrame:
    """Compute composite-scored model rankings with cost efficiency for a single snapshot."""
    scored = _filter_scorable_models(df, snapshot_date)
    scored = _compute_normalized_scores(scored)
    scored = _assign_composite_rank(scored)
    scored = _compute_cost_efficiency(scored)
    result = _select_ranking_columns(scored)

    logger.info("Gold rankings computed — %d models ranked", result.count())
    return result



def compute_model_trends(df: DataFrame) -> DataFrame:
    """Compute metric deltas between consecutive snapshots per model."""
    df = df.withColumn(
        "avg_price",
        (F.col("input_1m_price") + F.col("output_1m_price")) / 2.0,
    )

    model_window = Window.partitionBy("model_id").orderBy("snapshot_date")

    df = df.withColumn("prev_snapshot_date", F.lag("snapshot_date", 1).over(model_window))
    df = df.withColumn("intelligence_previous", F.lag("analysis_ai_index", 1).over(model_window))
    df = df.withColumn("speed_previous", F.lag("output_tokens_per_sec", 1).over(model_window))
    df = df.withColumn("price_previous", F.lag("avg_price", 1).over(model_window))

    df = df.withColumn("intelligence_delta", F.col("analysis_ai_index") - F.col("intelligence_previous"))
    df = df.withColumn("speed_delta", F.col("output_tokens_per_sec") - F.col("speed_previous"))
    df = df.withColumn("price_delta", F.col("avg_price") - F.col("price_previous"))

    result = df.select(
        "model_id",
        "model_name",
        "vendor",
        "snapshot_date",
        "prev_snapshot_date",
        F.col("analysis_ai_index").alias("intelligence_current"),
        "intelligence_previous",
        "intelligence_delta",
        F.col("output_tokens_per_sec").alias("speed_current"),
        "speed_previous",
        "speed_delta",
        F.col("avg_price").alias("price_current"),
        "price_previous",
        "price_delta",
    )

    logger.info("Gold trends computed — %d rows produced", result.count())
    return result
