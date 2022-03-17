from pyspark.sql.types import *
from pyspark.sql.functions import split
from kafka_to_spark_streaming import sdfIBM

ActionSchema = StructType([
    StructField("ActionId", ShortType()),
    StructField("Symbol", StringType()),
    StructField("LastRefreshed", TimestampType())])


def parse_data_from_kafka_message(sdf, schema):
    assert sdf.isStreaming == True, "DataFrame doesn't receive streaming data"
    col = split(sdf['value'], ',')  # split attributes to nested array in one Column
    # now expand col to multiple top-level columns
    for idx, field in enumerate(schema):
        sdf = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))
        return sdf.select([field.name for field in schema])


sdfIBM = parse_data_from_kafka_message(sdfIBM, ActionSchema)
