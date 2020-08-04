"""
created by Elenath FGS
big data project for HIT course 2020. :)
"""

import os
import pandas as pd
import json
import math

import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.mllib.recommendation import MatrixFactorizationModel, ALS


class recommend_model:
    def __init__(self, sc, review_ids, user_ids, business_ids, rank=8, seed=5, iteration=10, regulation_param=0.1):
        self.rank = rank
        self.seed = seed
        self.iteration = iteration
        self.regulation_param = regulation_param

        assert isinstance(review_ids, pyspark.RDD)
        assert isinstance(user_ids, pyspark.RDD)
        assert isinstance(business_ids, pyspark.RDD)
        self.review_ids = review_ids
        self.user_ids = user_ids
        self.business_ids = business_ids

        assert isinstance(sc, pyspark.SparkContext)
        self.sc = sc  # spark_context

        self.unique_id_user = None
        self.unique_id_business = None
        self.ratings = None

        self.training_RDD = None
        self.validation_RDD = None
        self.test_RDD = None
        self.validation_for_predict_RDD = None
        self.test_for_predict_RDD = None

    def init_data(self):
        """
        初始化 DataProcessor传入的数据
        为 Alternating Least Squares (ALS) 模型划分训练集，验证集，和测试集
        :return:
        """
        # map to the form of (user_id, (business_id, stars))
        review_ids_mapped = self.review_ids.map(lambda x: (x[0], (x[1], x[2])))

        # 生成每个user和business和id唯一对应的RDD,形式为(unique id, user / business name)
        # (user_id, generated_unique_id)
        int_user_id_to_string = self.user_ids.map(lambda x: x[0]).distinct().zipWithUniqueId().cache()
        # (business_id, generated_unique_id)
        int_business_id_to_string = self.business_ids.map(lambda x: x[0]).distinct().zipWithUniqueId().cache()
        # (generated_unique_id, user_id)
        self.unique_id_user = int_user_id_to_string.map(lambda x: (x[1], x[0]))
        # self.unique_id_user.saveAsTextFile(
        #     "F:/MyDeskTop/BigData_Analysis/Big_Homework/Project/Models/unique_id_user")
        # (generated_unique_id, business_id)
        self.unique_id_business = int_business_id_to_string.map(lambda x: (x[1], x[0]))
        # self.unique_id_business.saveAsTextFile(
        #     "F:/MyDeskTop/BigData_Analysis/Big_Homework/Project/Models/unique_id_business")

        # self.user_ids.saveAsTextFile("F:/MyDeskTop/BigData_Analysis/Big_Homework/Project/Models/user_ids")
        # self.business_ids.saveAsTextFile("F:/MyDeskTop/BigData_Analysis/Big_Homework/Project/Models/business_ids")
        # self.review_ids.saveAsTextFile("F:/MyDeskTop/BigData_Analysis/Big_Homework/Project/Models/review_ids")
        '''
        print(self.unique_id_user.first())
        print(self.unique_id_business.first())
        print(review_ids_mapped.first())
        import sys
        sys.exit(0)
        '''

        # (user_id, (business_id, stars)) join (user_id, generated_unique_id)
        # = form (user_id, ((business_id, stars), generated_unique_id))
        # final form (generated_unique_id,(business_id, stars))
        user_join_review = review_ids_mapped.join(int_user_id_to_string).map(lambda x: (x[1][1], x[1][0]))

        '''
        print("user_join_review"+str(user_join_review.first()))
        print("int_business_id_to_string"+str(int_business_id_to_string.first()))
        import sys
        sys.exit(0)
        '''

        # (business_id, generated_unique_id(business)) join (business_id, (generated_unique_id(user), stars))
        # = (business_id, (generated_unique_id(business), (generated_unique_id(user), stars))
        # ratings = (generated_unique_id(user), generated_unique_id(business), stars)
        self.ratings = int_business_id_to_string.join(user_join_review.map(lambda x: (x[1][0], (x[0], x[1][1])))).map(
            lambda x: (x[1][1][0], x[1][0], x[1][1][1]))

        print(len(self.ratings.collect()))

        # 划分训练集，验证集，测试集，划分比例为6:2:2
        assert isinstance(self.ratings, pyspark.RDD)
        self.training_RDD, self.validation_RDD, self.test_RDD = self.ratings.randomSplit([0.6, 0.2, 0.2], seed=0)

        # 截取不带标签的数据作为验证和测试输入数据
        # (generated_unique_id(user), generated_unique_id(business))
        self.validation_for_predict_RDD = self.validation_RDD.map(lambda x: (x[0], x[1]))
        self.test_for_predict_RDD = self.test_RDD.map(lambda x: (x[0], x[1]))

    def train_ASL_model(self, save_path):
        # 训练模型
        ALS_model = ALS.train(self.ratings, self.rank, self.iteration, lambda_=self.regulation_param)
        ALS_model.save(sc=self.sc, path=save_path)

    def model_evaluation(self, MF_model_path, save_path=None):
        # load Matrix Factorization Model
        MF_model = MatrixFactorizationModel.load(self.sc, MF_model_path)

        predictions = MF_model.predictAll(self.validation_for_predict_RDD).map(lambda x: ((x[0], x[1]), x[2]))

        # (gen_user_id, gen_busi_id, (true_rate, pred_rate))
        rate_pred = self.validation_RDD.map(lambda x: ((int(x[0]), int(x[1])), float(x[2]))).join(predictions)

        # evaluate model using MSE loss
        MSE_error = math.sqrt((rate_pred.map(lambda r: (r[1][0] - r[1][1]) ** 2)).mean())
        print(f"Model MSE loss = {MSE_error}")

        if save_path is not None:
            MF_model.save(self.sc, save_path)

    def predict(self, MF_model_path, user_id, using_local=True,
                save_location="F:/MyDeskTop/BigData_Analysis/Big_Homework/Project/Models/"):
        """
        对某用户进行系统推荐
        :param save_location: where to save and load the data
        :param using_local: using local file
        :param MF_model_path: the path where the trained MF model is stored
        :param user_id: the user id to be recommend
        :return:
        """

        MF_model = MatrixFactorizationModel.load(self.sc, MF_model_path)

        # (generated_unique_id(user), product, rating)
        recommend_results = MF_model.recommendProducts(user_id, 10)

        # (generated_unique_id(user), (product, rating))
        pred1 = self.sc.parallelize(recommend_results).map(lambda x: (x[0], (x[1], x[2])))

        # (generated_unique_id(user), user_id) join (generated_unique_id(user), (product, rating))
        if using_local:
            user_ids_to_string_replaced = self.sc.textFile(save_location + "unique_id_user").join(pred1)
            replace_both = user_ids_to_string_replaced.keyBy(lambda x: x[1][1][0]).join(
                self.sc.textFile(save_location + "unique_id_business")).map(
                lambda x: (x[1][0][1][0], x[1][1], x[1][0][1][1][1]))
            user_id = replace_both.map(lambda x: (x[0], (x[1], x[2]))).join(
                self.sc.textFile(save_location + "user_ids")).map(
                lambda x: (x[1][1][0], x[1][0]))
            business_id = user_id.map(lambda x: (x[1][0], (x[0], x[1][1]))).join(
                self.sc.textFile(save_location + "business_ids")).map(
                lambda x: (x[1][0][0], x[1][1][0]))
            assert isinstance(business_id, pyspark.RDD)
            return business_id.collect()
        else:
            user_ids_to_string_replaced = self.unique_id_user.join(pred1)  # replacing user ids
            replace_both = user_ids_to_string_replaced.keyBy(lambda x: x[1][1][0]).join(self.unique_id_business).map(
                lambda x: (x[1][0][1][0], x[1][1], x[1][0][1][1][1]))
            user_id = replace_both.map(lambda x: (x[0], (x[1], x[2]))).join(self.user_ids).map(
                lambda x: (x[1][1][0], x[1][0]))
            business_id = user_id.map(lambda x: (x[1][0], (x[0], x[1][1]))).join(self.business_ids).map(
                lambda x: (x[1][0][0], x[1][1][0]))
            assert isinstance(business_id, pyspark.RDD)
            return business_id.collect()
