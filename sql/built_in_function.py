from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("built-in function").getOrCreate()

    spark.sql("select array(1, 2, 3);").show()
    spark.sql("select array_contains(array(1, 2, 3), 2);").show()
    spark.sql("select array_distinct(array(1, '2', 3, null, '3'))").show()
    spark.sql("select array_except(array(1, 2, 3), array(1, 3, 5))").show()  # 返回第一个数组中和第二个数组不重复的元素
    spark.sql("select array_intersect(array(1, 2, 3), array(1, 3, 5));").show()  # 返回两个数组交集-在数组1和数组2都出现的数组
    spark.sql(" SELECT sequence(1, 5);").show()
    spark.sql(" SELECT sequence(5, 1);").show()

if __name__ == '__main__':
    main()
