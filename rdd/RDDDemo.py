from pyspark import SparkConf, SparkContext, RDD


def main():
    config = SparkConf().setAppName('RDD Demo').setMaster("local")
    sc = SparkContext(conf=config)

    items = ["beijing", "beijing", "beijing", "shanghai", "shanghai", "tianjing", "tianjing"]

    cities = sc.parallelize(items)

    cities_map = cities.map(lambda x: (x, 1))
    reduce_result = cities_map.reduceByKey(lambda x, y: x + y)
    reduce_result.foreach(lambda x: print(x))


if __name__ == '__main__':
    main()
