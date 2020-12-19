from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("Simple").getOrCreate()

    df_json = spark.read.json("/Users/risheng/dminer/spark/dminer_spark/resource/people.json")
    df_json.show()

    df_json.printSchema()

    df_json.select("name", 'age').show()

    df_json.groupBy("name").count().show()

    df_json.groupBy("name").sum("age").show()

    df_json.createOrReplaceTempView("people")

    spark.sql("select * from people where age>20").show()

    df_json.createGlobalTempView("global_people")

    spark.sql("select name, age from global_temp.global_people").show()

if __name__ == '__main__':
    main()
