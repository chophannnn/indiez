from pyspark.sql import (
  SparkSession,
  DataFrame
)
from pyspark.sql.functions import (
  col,
  regexp_extract,
  from_unixtime,
  date_format,
  min,
  asc
)

def main(path: str) -> None:
  spark = (
    SparkSession
      .builder
      .getOrCreate()
  )

  processing: DataFrame = (
    spark
      .read
      .format("parquet")
      .load(path)
      .withColumn(
        "{project_id}",
        regexp_extract(col("{campaign_name}"), "(_)([A-Z]{2}[a-z]{1})(_)", 2)
      )
  )

  earliest_recorded_event_time_by_project_id: list[dict[str, str]] = (
    processing
      .filter(
        (col("{project_id}").isNotNull())
        & (col("{project_id}") != '')
      )
      .withColumn(
        "{created_at}",
        date_format(from_unixtime(col("{created_at}")), "yyyy-MM-dd")
      )
      .groupBy("{project_id}")
      .agg(min(col("{created_at}")).alias("{created_at}"))
      .orderBy(asc(col("{created_at}")))
      .rdd
      .map(
        lambda x: {
          "project_id": x["{project_id}"],
          "created_at": x["{created_at}"]
        }
      )
      .collect()
  )

  print(earliest_recorded_event_time_by_project_id)

  spark.stop()

if __name__ == "__main__":
  path: str = "/home/spark/processing.parquet"

  main(path)
