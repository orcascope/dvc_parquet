from pyspark.sql import SparkSession
import sys
if __name__ == "__main__":
    spark= SparkSession.builder.master("local").getOrCreate()
    df = spark.read.format("csv").option("header","true").load("/home/arshadh/Downloads/traveler_statedisc_licensure_rankinflu_202209220829.csv")
    version = sys.argv[1]
    save_path = sys.argv[2]
    if version==1:
        version=0
    df.filter("travelerid%"+str(version)+"==0").write.format("parquet").mode("overwrite").save(save_path)