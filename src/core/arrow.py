import pyarrow as pa
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

_SPARK_TO_ARROW = {
    StringType: pa.string(),
    DoubleType: pa.float64(),
    FloatType: pa.float32(),
    IntegerType: pa.int32(),
    LongType: pa.int64(),
    DateType: pa.date32(),
    BooleanType: pa.bool_(),
    TimestampType: pa.timestamp("us"),
}


def spark_to_arrow_schema(spark_schema) -> pa.Schema:
    """Convert a Spark StructType to a PyArrow schema preserving column types."""
    fields = []
    for field in spark_schema.fields:
        arrow_type = _SPARK_TO_ARROW.get(type(field.dataType))
        if arrow_type is None:
            raise ValueError(f"Unsupported Spark type for Arrow conversion: {field.dataType}")
        fields.append(pa.field(field.name, arrow_type, nullable=field.nullable))
    return pa.schema(fields)
