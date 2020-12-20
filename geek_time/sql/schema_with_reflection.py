# from pyspark import SparkConf, SparkContext
from pyspark.sql import Row
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("Demo").getOrCreate()

    sc = spark.sparkContext

    lines = sc.textFile("/Users/risheng/dminer/spark/dminer_spark/resource/people.txt")
    print(type(lines))
    lines.foreach(lambda l: print(l))

    paris = lines.map(lambda l: l.split(","))
    paris.foreach(lambda p: print(p))

    people = paris.map(lambda p: Row(name=p[0], age=int(p[1])))

    schemaPeople = spark.createDataFrame(people)
    print(type(schemaPeople))
    schemaPeople.createOrReplaceTempView("people")

    teenagers = spark.sql("select * from people where age between 13 and 19")

    print(type(teenagers))
    print(type(teenagers.rdd))

    teenNames = teenagers.rdd.map(lambda p: f"Name: {p.name}").collect()
    for name in teenNames:
        print(name)


if __name__ == '__main__':
    main()
