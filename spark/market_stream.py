from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    window,
    avg,
    sum as _sum,
    last,
    to_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    TimestampType,
)
import os
import time

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "market.ticks")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "marketdb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_URL = os.getenv(
    "POSTGRES_URL", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

CHECKPOINT = os.getenv("SPARK_CHECKPOINT", "/tmp/checkpoints/market_stream")

spark = SparkSession.builder.appName("market-stream").getOrCreate()

schema = StructType(
    [
        StructField("event_type", StringType()),
        StructField("symbol", StringType()),
        StructField("price", DoubleType()),
        StructField("volume", LongType()),
        StructField("event_time", StringType()),
        StructField("received_time", StringType()),
        StructField("source", StringType()),
    ]
)

raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

json_df = (
    raw_df.selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .withColumn(
        "event_time", to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSX")
    )
    .withColumn(
        "received_time",
        to_timestamp(col("received_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
    )
)

# aggregate metrics per 1-minute window
agg = (
    json_df.withWatermark("event_time", "5 minutes")
    .groupBy(col("symbol"), window(col("event_time"), "1 minute"))
    .agg(
        avg("price").alias("moving_avg_1min"),
        _sum("volume").alias("vol_1min"),
        last("price", ignorenulls=True).alias("last_price"),
    )
)

out = agg.select(
    col("symbol"),
    col("last_price"),
    col("moving_avg_1min"),
    col("vol_1min"),
    col("window.end").alias("updated_at"),
)


# helper: write symbol_metrics using psycopg2 per partition (upsert)
def write_metrics_partition(rows):
    import psycopg2
    from psycopg2.extras import execute_batch
    import os

    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
    POSTGRES_DB = os.getenv("POSTGRES_DB", "marketdb")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

    conn = None
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            port=POSTGRES_PORT,
        )
        cur = conn.cursor()
        data = []
        for r in rows:
            data.append(
                (
                    r.symbol,
                    float(r.last_price) if r.last_price is not None else None,
                    float(r.moving_avg_1min) if r.moving_avg_1min is not None else None,
                    int(r.vol_1min) if r.vol_1min is not None else 0,
                    r.updated_at,
                )
            )
        if data:
            sql = """
            INSERT INTO symbol_metrics (symbol, last_price, moving_avg_1min, vol_1min, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (symbol) DO UPDATE SET
                last_price = EXCLUDED.last_price,
                moving_avg_1min = EXCLUDED.moving_avg_1min,
                vol_1min = EXCLUDED.vol_1min,
                updated_at = EXCLUDED.updated_at;
            """
            execute_batch(cur, sql, data)
            conn.commit()
        if conn:
            cur.close()
    except Exception as e:
        # log error to stdout (Spark worker logs)
        print("Error writing metrics partition:", e)
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass


# foreachBatch function: write ticks via JDBC append and metrics via foreachPartition upsert
def foreach_batch_function(batch_df, epoch_id):
    # write raw ticks to 'ticks' table using JDBC (requires JDBC driver in spark/jars)
    try:
        ticks_df = batch_df.select(
            "symbol", "price", "volume", "source", "event_time", "received_time"
        )
        if ticks_df.rdd.isEmpty():
            print("No rows in ticks_df for epoch", epoch_id)
        else:
            ticks_df.write.format("jdbc").option("url", POSTGRES_URL).option(
                "dbtable", "ticks"
            ).option("user", POSTGRES_USER).option("password", POSTGRES_PASSWORD).mode(
                "append"
            ).save()
    except Exception as e:
        print("Error writing ticks via JDBC:", e)

    # prepare metrics df (aggregated) - this function expects the 'out' format
    try:
        # Convert to local row objects and upsert per partition
        batch_df.foreachPartition(write_metrics_partition)
    except Exception as e:
        print("Error writing metrics:", e)


# Note: use update mode to get updated aggregates
query = (
    out.writeStream.foreachBatch(foreach_batch_function)
    .outputMode("update")
    .option("checkpointLocation", CHECKPOINT)
    .start()
)

query.awaitTermination()
