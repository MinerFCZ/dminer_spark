from pyspark import SparkConf, SparkContext, TaskContext


def print_partition(item):
    print(f"partition id: {TaskContext.get().partitionId()}")
    for i in item:
        print(i)
    print('--------分割线---------')


def map_partitions_func(items):
    item_num = 0
    for i in items:
        item_num = item_num + 1
        # print(i)

    yield item_num


def map_partitions_index_func(index, data):
    yield index
     # map_partitions_func(data)


def main():
    conf = SparkConf().setAppName("RDD Transformation").setMaster("local")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile("/Users/risheng/dminer/spark/dminer_spark/resource/simple", 2)
    # rdd.foreach(lambda x:print(x))
    rdd.foreachPartition(print_partition)

    rdd.flatMap(lambda x: x.split(" ")) \
        .map(lambda x: (x, 1)) \
        .foreach(lambda x: print(f"flatMap-->map: {x}"))

    rdd.flatMap(lambda x: x.split(" ")) \
        .filter(lambda x: x != 'bag') \
        .foreach(lambda x: print(f"flatMap-->filter: {x}"))

    print("flatMap-->collect:{0}".format(rdd.flatMap(lambda x: x.split(" ")).collect()))

    print("mapPartitions-->collect:{0}".format(rdd.flatMap(lambda x: x.split(" ")) \
                                               .mapPartitions(map_partitions_func).collect()))

    print("mapPartitionsWithIndex-->collect: {0}".format(rdd.flatMap(lambda x: x.split(' '))
                    .mapPartitionsWithIndex(map_partitions_index_func)
                    .sum()))
    # rdd.flatMap(lambda x: x.split(" ")).mapPartitionsWithIndex(lambda i, d: print(f"index: {i} \t data:{d}"))


if __name__ == '__main__':
    main()
