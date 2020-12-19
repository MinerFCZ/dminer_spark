from pyspark.sql import SparkSession
from pyspark.sql.types import *


def main():
    spark = SparkSession.builder.appName("Programmatically Specifying the Schema") \
        .getOrCreate()

    sc = spark.sparkContext

    lines = sc.textFile("/Users/risheng/dminer/spark/dminer_spark/resource/people.txt")
    parts = lines.map(lambda l: l.split(","))

    people = parts.map(lambda p: (p[0], p[1].strip()))

    schema_str = "name age"
    fields = [StructField(field_name, StringType(), True) for field_name in schema_str.split()]
    print(type(fields))

    schema = StructType(fields)
    print((type(schema)))

    schema_people = spark.createDataFrame(people, schema)

    schema_people.createOrReplaceTempView("people")

    spark.sql("select name from people").show()


if __name__ == '__main__':
    main()
