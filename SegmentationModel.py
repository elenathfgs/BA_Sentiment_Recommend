"""
created by Elenath FGS
big data project for HIT course 2020. :)
"""

import os
import pandas as pd
import json
import re
import string

import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from operator import add
from pyspark.sql import functions
from pyspark.ml.feature import Tokenizer, StopWordsRemover, NGram, CountVectorizer, IDF
from pyspark.mllib.classification import SVMWithSGD
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import Vector as MLLibVector, Vectors as MLLibVectors
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


def clean_text(review_text):
    """
    移除review中的标点符号和数字及换行回车制表符
    :param review_text: 评价文本
    :return: cleaned_text
    """
    regex = re.compile("[" + re.escape(string.punctuation) + "0-9\\r\\t\\n]")
    cleaned_text = regex.sub(" ", review_text)
    return cleaned_text


def convert_stars(stars):
    """
    评价情绪两极化处理
    :param stars: 打星指标
    :return: 1正面 0负面
    """
    stars = int(stars)
    return 1 if stars >= 4 else 0


class segmentation_model:
    def __init__(self):
        self.data_root = "./Yelp_Data"
        self.spark = SparkSession \
            .builder \
            .appName("Python Spark SQL") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()
        self.cv_model = None
        self.svm = None

        self.segment_box = []

    def read_data(self, type_="review"):
        """
        读取原始数据，计算三单词元组的频繁度，并用于替换原来的评价文本
        :param type_: 数据来源的类型
        :return: nothing
        """
        data_name = f"yelp_academic_dataset_{type_}.json"
        review_frame = self.spark.read.json(os.path.join(self.data_root, data_name))

        # use user defined functions
        text_cleaner = udf(lambda x: clean_text(x))
        stars_converter = udf(lambda x: convert_stars(x))

        review_data = review_frame.select('review_id', text_cleaner('text'), stars_converter('stars'))

        review_data = review_data.withColumnRenamed('<lambda>(text)', 'text'). \
            withColumn('label', review_data["<lambda>(stars)"].cast(IntegerType())). \
            drop('<lambda>(stars)').limit(100000)  # choose 1000000 data

        # extract the words in the text
        token_transfer = Tokenizer(inputCol="text", outputCol="words")
        review_tokens = token_transfer.transform(review_data)

        # delete stop words
        delete_stopword_transfer = StopWordsRemover(inputCol="words", outputCol="useful_words")
        review_useful_words = delete_stopword_transfer.transform(review_tokens)

        # review_useful_words.show(8)

        ngram = NGram(inputCol="words", outputCol="ngram", n=3)
        with_ngram = ngram.transform(review_useful_words)
        assert isinstance(with_ngram, pyspark.sql.DataFrame)
        # with_ngram.show(5)  # 在报告中有

        # find the ngram with frequency >= 15
        ngrams = with_ngram.rdd.flatMap(lambda x: x[-1]). \
            filter(lambda x: len(x.split()) == 3)  # only use ngram attribute

        # descend sort the ngram by appearing times and choose those appear more than 20 times
        ngram_statistic = ngrams.map(lambda x: (x, 1)).reduceByKey(add).sortBy(lambda x: x[1], ascending=False).filter(
            lambda x: x[1] >= 15)

        ngram_list = ngram_statistic.map(lambda x: x[0]).collect()  # x[1] is the frequency!
        print(ngram_list)

        # def connect_ngram(text):
        #     """
        #     用‘_’对三单词进行连接，用于替换原文本的单词
        #     :param text: 原始文本
        #     :return: 替换后的文本
        #     """
        #     text_low = text.lower()
        #     for ngram in ngram_list:
        #         return text_low.replace(ngram, ngram.replace(" ", "_"))

        # ngram_udf = udf(lambda x: connect_ngram(x))
        # ngram_udf = review_useful_words.select(ngram_udf('text'), 'label').withColumnRenamed('<lambda>(text)', 'text')
        # tokenized_ngram = tokenizer.transform(ngram_udf)
        # tokenized_ngram = delete_stopword_transfer.transform(tokenized_ngram)

        tokenizer = Tokenizer(inputCol='text', outputCol='words')
        review_tokenized = tokenizer.transform(review_data)
        review_tokenized = delete_stopword_transfer.transform(review_tokenized)

        # transfer to the count vectorizer and tfidf
        count_vector = CountVectorizer(inputCol='useful_words', outputCol='vectors')
        self.cv_model = count_vector.fit(review_tokenized)
        count_vectorized = self.cv_model.transform(review_tokenized)
        count_vectorized.show(10)

        idf = IDF().setInputCol('vectors').setOutputCol('idf_output')
        idf_model = idf.fit(count_vectorized)
        idf_df = idf_model.transform(count_vectorized)

        assert isinstance(idf_df, pyspark.sql.DataFrame)
        idf_df.show(10)

        return idf_df

    @staticmethod
    def train_SVM(idf_df, iterations=50, regress_param=0.3):
        """
        通过上面划分的数据向量来训练SVM
        注：这里必须是静态方法，否则会出现sparkContext广播错误（sparkContext只能由全局driver使用）
        :param idf_df:
        :param iterations:
        :param regress_param:
        :return:
        """
        splits = idf_df.select(['idf_output', 'label']).randomSplit([0.8, 0.2], seed=100)
        train = splits[0].cache()
        test = splits[1].cache()

        train_lb = train.rdd.map(lambda row: LabeledPoint(row[1], MLLibVectors.fromML(row[0])))
        # SVM model
        svm = SVMWithSGD.train(train_lb, iterations, regParam=regress_param)

        test_lb = test.rdd.map(lambda row: LabeledPoint(row[1], MLLibVectors.fromML(row[0])))
        scoreAndLabels_test = test_lb.map(lambda x: (float(svm.predict(x.features)), x.label))
        spark = SparkSession \
            .builder \
            .appName("Python Spark SQL") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()
        score_label_test = spark.createDataFrame(scoreAndLabels_test, ["prediction", "label"])

        # F1 score
        f1_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
        svm_f1 = f1_eval.evaluate(score_label_test)
        print("F1 score: %.4f" % svm_f1)
        return svm

    def evaluate(self, svm, save_path=None):
        vocabulary_ngram = self.cv_model.vocabulary
        print(vocabulary_ngram)
        weights_ngram = svm.weights.toArray()
        print(weights_ngram)
        svm_coeffs_df_ngram = pd.DataFrame({'ngram': vocabulary_ngram, 'weight': weights_ngram})

        result_negative = svm_coeffs_df_ngram.sort_values('weight').head(30)
        # print(result_positive[['ngram','weight']].values)
        for ngram_weight in result_negative[['ngram', 'weight']].values:
            self.segment_box.append((ngram_weight[0], ngram_weight[1]))

        result_positive = svm_coeffs_df_ngram.sort_values('weight', ascending=False).head(30)
        # print(result_negative)
        for ngram_weight in result_positive[['ngram', 'weight']].values:
            self.segment_box.append((ngram_weight[0], ngram_weight[1]))

        if save_path is not None:
            n_save_path = os.path.join(save_path, "negative.csv")
            result_negative.to_csv(n_save_path)
            p_save_path = os.path.join(save_path, "positive.csv")
            result_positive.to_csv(p_save_path)


def segment_predict(review_str, positive_path, negative_path):
    assert isinstance(review_str, str)

    positive_df = pd.read_csv(positive_path)
    negative_df = pd.read_csv(negative_path)

    sentiments = {}
    for word_score in positive_df.values:
        sentiments[word_score[1]] = word_score[2]
    for word_score in negative_df.values:
        sentiments[word_score[1]] = word_score[2]
    review_words = review_str.split(" ")
    score = 0
    for word in review_words:
        if word in sentiments.keys():
            score += sentiments[word]

    return round(score, 3)


if __name__ == "__main__":
    segmentationModel = segmentation_model()
    idf_df = segmentationModel.read_data(type_="review10000")
    svm = segmentation_model.train_SVM(idf_df)
    segmentationModel.evaluate(svm, save_path=None) # save_path="./Segment"

    # review = "this restaurant is great, the food is delicious and the waiters are awesome, " \
    #          "they are friendly let alone that they are sometimes slow and inefficient"
    # score = segment_predict(review, "./Segment/positive.csv", "./Segment/negative.csv")
    # print(score)
