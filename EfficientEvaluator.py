"""
created by Elenath FGS
big data project for HIT course 2020. :)
"""

import os
import pandas as pd
import json
import numpy as np

from pyspark import SparkContext
from tools import extract_data
import matplotlib.pyplot as plt
from SegmentationModel import segmentation_model
from main import *
import time


def recommend_test():
    data_volumes = [5000, 10000, 15000, 20000, 25000, 30000, 35000, 40000, 45000]
    time_costs = []

    for data_volume in data_volumes:
        data_path = "./Yelp_Data"
        model_path = "F:/MyDeskTop/BigData_Analysis/Big_Homework/Project/Models/ASL_model{0}".format(data_volume)
        if not os.path.exists(model_path):
            os.mkdir(model_path)
        sc = SparkContext(master="local", appName="BigDataHomework")

        start = time.time()
        review_file_path = os.path.join(data_path, "yelp_academic_dataset_review{0}.json".format(data_volume))
        user_file_path = os.path.join(data_path, "yelp_academic_dataset_user{0}.json".format(data_volume))
        business_file_path = os.path.join(data_path, "yelp_academic_dataset_business{0}.json".format(data_volume))

        review_ids, user_ids, business_ids = get_recommend_profiles(sc, review_file_path, user_file_path,
                                                                    business_file_path)
        recommendModel = recommend_model(sc, review_ids, user_ids, business_ids)
        recommendModel.init_data()
        recommendModel.train_ASL_model(save_path=model_path)
        recommendModel.model_evaluation(MF_model_path=model_path)
        sc.stop()
        end = time.time()

        time_costs.append(end - start)

    width = 0.5  # the width of the bars
    x = np.arange(len(data_volumes))
    fig, ax = plt.subplots()
    ax.bar(x, time_costs, width=width, alpha=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(data_volumes)
    ax.set_ylabel('Time Cost (s)')
    ax.set_title("recommend module test")

    fig.tight_layout()
    plt.show()


def segment_test():
    data_volumes = [1000, 3000, 5000, 7000, 9000, 12000, 15000, 20000, 25000, 30000, 35000, 40000]
    time_costs = []
    data_path = "./Yelp_Data"
    review_data_path = data_path + "/yelp_academic_dataset_review{0}.json"

    for data_volume in data_volumes:
        print("test volume {0}".format(data_volume))
        start_time = time.time()
        if not os.path.exists(review_data_path.format(data_volume)):
            extract_data(review_data_path.format(""), review_data_path.format(data_volume), data_volume)
        segmentationModel = segmentation_model()
        idf_df = segmentationModel.read_data(type_="review{0}".format(data_volume))
        svm = segmentation_model.train_SVM(idf_df)
        segmentationModel.evaluate(svm, save_path=None)  # save_path="./Segment"
        segmentationModel.spark.stop()

        end_time = time.time()

        time_costs.append(end_time - start_time)

    width = 0.5  # the width of the bars
    x = np.arange(len(data_volumes))
    fig, ax = plt.subplots()
    ax.bar(x, time_costs, width=width, alpha=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(data_volumes)
    ax.set_ylabel('Time Cost (s)')
    ax.set_title("segment module test")

    fig.tight_layout()
    plt.show()


def read_RDD_test():
    data_path = "./Yelp_Data"
    review_data_path = data_path + "/yelp_academic_dataset_review{0}.json"
    user_data_path = data_path + "/yelp_academic_dataset_user{0}.json"
    business_data_path = data_path + "/yelp_academic_dataset_business{0}.json"

    data_volumes = [1000, 5000, 10000, 15000, 20000, 25000, 30000, 35000, 40000, 45000, 50000, 100000]
    review_time = []
    user_time = []
    business_time = []

    # prepare files
    for volume in data_volumes:
        print(f"calculate volume {volume}")

        test_review_data_path = review_data_path.format(volume)
        if not os.path.exists(test_review_data_path):
            extract_data(review_data_path.format(""), test_review_data_path, volume)
        cost = get_read_time(test_review_data_path)
        review_time.append(cost)

        test_user_data_path = user_data_path.format(volume)
        if not os.path.exists(test_user_data_path):
            extract_data(user_data_path.format(""), test_user_data_path, volume)
        cost = get_read_time(test_user_data_path)
        user_time.append(cost)

        test_business_data_path = business_data_path.format(volume)
        if not os.path.exists(test_business_data_path):
            extract_data(business_data_path.format(""), test_business_data_path, volume)
        cost = get_pandas_read_time(test_business_data_path)
        business_time.append(cost)

    print(review_time)
    print(user_time)
    print(business_time)

    fig, ax = plt.subplots()
    ax.plot(data_volumes[1:], review_time[1:], label='review_RDD')
    ax.plot(data_volumes[1:], user_time[1:], label='user_RDD')
    ax.plot(data_volumes[1:], business_time[1:], label='business_RDD')
    ax.set_xlabel("data volume")
    ax.set_ylabel("time cost (s)")
    ax.set_title("file read test")
    ax.legend()
    plt.show()


def cache_test():
    import numpy as np
    data_path = "./Yelp_Data"
    review_data_path = data_path + "/yelp_academic_dataset_review{0}.json"
    data_volumes = [1000, 5000, 10000, 15000, 20000, 25000, 30000, 35000, 40000, 45000, 50000, 100000]
    costs = []
    costs_cache = []

    for volume in data_volumes:
        print(f"calculate volume {volume}")
        cost = get_cache_time(review_data_path.format(volume), False)

        cost_cache = get_cache_time(review_data_path.format(volume), True)
        costs.append(cost)
        costs_cache.append(cost_cache)

    print(costs_cache)
    print(costs)

    width = 0.35  # the width of the bars
    x = np.arange(len(data_volumes))
    fig, ax = plt.subplots()
    ax.bar(x, costs, width=width, label='without cache', alpha=0.5)
    ax.bar(x + width, costs_cache, width=width, label="with cache", alpha=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(data_volumes)
    ax.set_ylabel('Time Cost')
    ax.set_title("cache test")
    ax.legend()

    fig.tight_layout()
    plt.show()


def get_cache_time(file_path, cache):
    """
    test for review file
    :param file_path:
    :param cache:
    :return:
    """
    import time
    from operator import add
    sc = SparkContext(master="local", appName="BigDataHomework")
    start = time.time()
    review_raw_RDD = sc.textFile(file_path)
    review_json_RDD = review_raw_RDD.map(lambda line: json.loads(line))
    if cache:
        review_ids = review_json_RDD.map(lambda line: (line['user_id'], line['stars'])).reduceByKey(add).cache()
    else:
        review_ids = review_json_RDD.map(lambda line: (line['user_id'], line['stars'])).reduceByKey(add)
    new_review_ids = review_json_RDD.map(
        lambda line: (line['user_id'], line['business_id'], line['stars'], line['text']))
    busi_star = new_review_ids.map(lambda x: (x[1], x[2]))
    busi_star_redu = busi_star.reduceByKey(add)

    # reuse the result
    a = review_ids.map(lambda x: x[1])
    a1 = a.collect()

    b = review_ids.map(lambda x: x[1] ** 2).count()
    c = review_ids.filter(lambda x: len(x[0]) > 8).collect()

    d = review_ids.map(lambda x: str(x[0] + "is the new user id"))
    e = review_ids.filter(lambda line: line[1] > 3).map(lambda x: x[0]).collect()
    f = review_ids.map(lambda x: str(x[0] + "is the reuse test")).collect()

    d = review_ids.map(lambda x: str(x[0] + "is not the new user id"))
    e = review_ids.filter(lambda line: line[1] > 1).map(lambda x: x[0]).collect()
    f = review_ids.map(lambda x: str(x[0] + "is not the reuse test")).collect()

    d = review_ids.map(lambda x: str(x[0] + "is of course not the new user id"))
    e = review_ids.filter(lambda line: line[1] > 2).map(lambda x: x[0]).collect()
    f = review_ids.map(lambda x: str(x[0] + "is some sort of the reuse test")).collect()

    # b = review_ids.map(lambda x: x[1] ** 3).count()
    # c = review_ids.filter(lambda x: len(x[0]) > 6).collect()
    #
    # b = review_ids.map(lambda x: x[1] + 3).count()
    # c = review_ids.filter(lambda x: len(x[0]) > 5).collect()
    #
    # d = review_ids.map(lambda x: str(x[0] + "is ohhhhhhhh the new user id"))
    # e = review_ids.filter(lambda line: line[1] > 1).map(lambda x: x[0]).collect()
    # f = review_ids.map(lambda x: str(x[0] + "is ohhhhhhhh the reuse test")).collect()
    #
    # d = review_ids.map(lambda x: str(x[0] + "is ohhhhhhhh not the new user id"))
    # e = review_ids.filter(lambda line: line[1] > 4).map(lambda x: x[0]).collect()
    # f = review_ids.map(lambda x: str(x[0] + "is ohhhhhhhh not the reuse test")).collect()
    #
    # d = review_ids.map(lambda x: str(x[0] + "is ohhhhhhhh of course not the new user id"))
    # e = review_ids.filter(lambda line: line[1] > 0).map(lambda x: x[0]).collect()
    # f = review_ids.map(lambda x: str(x[0] + "is ohhhhhhhh some sort of the reuse test")).collect()
    #
    # b = review_ids.map(lambda x: x[1] + 5).count()
    # c = review_ids.filter(lambda x: len(x[0]) > 9).collect()

    end = time.time()
    sc.stop()

    return end - start


def get_read_time(file_path):
    import time
    sc = SparkContext(master="local", appName="BigDataHomework")
    start = time.time()
    raw_RDD = sc.textFile(file_path)
    raw_RDD.map(lambda line: json.loads(line))
    end = time.time()
    sc.stop()

    return end - start


def get_pandas_read_time(file_path):
    import time
    start = time.time()
    with open(file_path, "r", encoding="utf-8") as f:
        a = f.read()
    end = time.time()
    b = a
    return end - start


if __name__ == "__main__":
    recommend_test()
