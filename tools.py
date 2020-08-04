"""
executable script
created by Elenath FGS
big data project for HIT course 2020. :)
"""


def extract_data(origin_file_path, out_file_path, line_num):
    with open(out_file_path, "a", encoding="utf-8") as out_file:
        with open(origin_file_path, "r", encoding="utf-8") as source_file:
            origin_line = source_file.readline()
            count = 0
            while origin_line != "" and count < line_num:
                out_file.write(origin_line)
                count += 1
                origin_line = source_file.readline()


def test_pyspark():
    from pyspark import SparkConf, SparkContext
    # 创建SparkConf和SparkContext
    conf = SparkConf().setMaster("local").setAppName("lichao-wordcount")
    sc = SparkContext(conf=conf)
    # 输入的数据
    data = ["hello", "world", "hello", "word", "count", "count", "hello"]
    # 将Collection的data转化为spark中的rdd并进行操作
    rdd = sc.parallelize(data)
    resultRdd = rdd.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    # rdd转为collecton并打印
    resultColl = resultRdd.collect()
    for line in resultColl:
        print(line)


if __name__ == "__main__":
    extractNum = 10000
    originFilePath = "./Yelp_Data/yelp_academic_dataset_review.json"
    out_file_path = f"./Yelp_Data/yelp_academic_dataset_review{extractNum}.json"
    extract_data(originFilePath, out_file_path, extractNum)
