"""
executable script
created by Elenath FGS
big data project for HIT course 2020. :)
"""

import os
import pandas as pd
import json

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions

from RecommendModel import recommend_model
from DataProcessor import get_recommend_profiles

if __name__ == "__main__":
    """
    initiate the spark context
    """
    data_path = "./Yelp_Data"
    model_path = "F:/MyDeskTop/BigData_Analysis/Big_Homework/Project/Models/ASL_model"
    sc = SparkContext(master="local", appName="BigDataHomework")
    sql_context = SQLContext(sc)

    review_file_path = os.path.join(data_path, "yelp_academic_dataset_review10000.json")
    user_file_path = os.path.join(data_path, "yelp_academic_dataset_user10000.json")
    business_file_path = os.path.join(data_path, "yelp_academic_dataset_business10000.json")

    review_ids, user_ids, business_ids = get_recommend_profiles(sc, review_file_path, user_file_path,
                                                                business_file_path)
    recommendModel = recommend_model(sc, review_ids, user_ids, business_ids)
    recommendModel.init_data()
    recommendModel.train_ASL_model(save_path=model_path)
    recommendModel.model_evaluation(MF_model_path=model_path)
    recommendModel.predict(model_path, 514, using_local=False, save=True)
