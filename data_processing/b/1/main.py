from pyspark.sql import (
  SparkSession,
  DataFrame
)
from pyspark.sql.functions import (
  col,
  regexp_extract
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

  processing.show(truncate=False)

  spark.stop()

if __name__ == "__main__":
  path: str = "/home/spark/processing.parquet"

  main(path)
