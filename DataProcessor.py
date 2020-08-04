"""
created by Elenath FGS
big data project for HIT course 2020. :)
"""

import os
import pandas as pd
import json

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions


def get_recommend_profiles(sc, review_file, user_file, business_file):
    """
    从原始的json文件中通过spark抽取需要的数据，并进行属性过滤
    :param sc: SparkContext instance
    :param review_file: review data file location
    :param user_file: user data file location
    :param business_file: business data file location
    :return: review_ids, user_ids, business_ids
    """
    # get review profiles
    review_raw_RDD = sc.textFile(review_file)
    # 将每一行数据的转化为json的格式
    review_json_RDD = review_raw_RDD.map(lambda line: json.loads(line))
    # 提取需要的attributes
    review_ids = review_json_RDD.map(lambda line: (line['user_id'], line['business_id'], line['stars'])).cache()

    # get user profiles
    user_raw_RDD = sc.textFile(user_file)
    # 将每一行数据的转化为json的格式
    user_json_RDD = user_raw_RDD.map(lambda line: json.loads(line))
    # 提取需要的attributes
    user_ids = user_json_RDD.map(lambda line: (line['user_id'], (line['name'], line['friends']))).cache()

    # get business profiles
    business_raw_RDD = sc.textFile(business_file)
    business_json_RDD = business_raw_RDD.map(lambda line: json.loads(line))
    business_ids = business_json_RDD.map(lambda line:
                                         (
                                             line['business_id'],
                                             (
                                                 line['name'], line['address'], line['categories'],
                                                 line['state'], line['city'], line['latitude'],
                                                 line['longitude'], line['stars']
                                             )
                                         )
                                         ).cache()
    return review_ids, user_ids, business_ids


if __name__ == "__main__":
    data_path = "./Yelp_Data"

    review_file_path = os.path.join(data_path, "yelp_academic_dataset_review1000.json")
    user_file_path = os.path.join(data_path, "yelp_academic_dataset_user1000.json")
    business_file_path = os.path.join(data_path, "yelp_academic_dataset_business1000.json")
    get_recommend_profiles(review_file_path, user_file_path, business_file_path)
