from pyspark.sql import (
  SparkSession,
  DataFrame
)
from pyspark.sql.functions import (
  col,
  regexp_extract,
  desc,
  count
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

  transaction_num_by_project_id: list[dict[str, str]] = (
    processing
      .filter(
        (col("{project_id}").isNotNull())
        & (col("{project_id}") != '')
      )
      .groupBy("{project_id}")
      .agg(count(col("*")).alias("{transaction_num}"))
      .orderBy(desc("{transaction_num}"))
      .rdd
      .map(
        lambda x: {
          "project_id": x["{project_id}"],
          "transaction_num": x["{transaction_num}"]
        }
      )
      .collect()
  )

  print(transaction_num_by_project_id)

  spark.stop()

if __name__ == "__main__":
  path: str = "/home/spark/processing.parquet"

  main(path)
