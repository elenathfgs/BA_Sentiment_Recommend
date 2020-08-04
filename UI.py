# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'UI.ui'
#
# Created by: PyQt5 UI code generator 5.13.0
#
# WARNING! All changes made in this file will be lost!


from PyQt5 import QtCore, QtGui, QtWidgets
import pandas as pd
import os
import json

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions

from RecommendModel import recommend_model
from DataProcessor import get_recommend_profiles


class Ui_mainWindow(object):
    def setupUi(self, mainWindow):
        mainWindow.setObjectName("mainWindow")
        mainWindow.resize(972, 771)
        self.centralwidget = QtWidgets.QWidget(mainWindow)
        self.centralwidget.setObjectName("centralwidget")
        self.label_recommend_title = QtWidgets.QLabel(self.centralwidget)
        self.label_recommend_title.setGeometry(QtCore.QRect(200, 10, 111, 41))
        self.label_recommend_title.setStyleSheet("font: 16pt \"华文琥珀\";")
        self.label_recommend_title.setObjectName("label_recommend_title")
        self.label_customer = QtWidgets.QLabel(self.centralwidget)
        self.label_customer.setGeometry(QtCore.QRect(20, 90, 91, 51))
        self.label_customer.setStyleSheet("font: 16pt \"华文琥珀\";")
        self.label_customer.setObjectName("label_customer")
        self.horizontalLayoutWidget_2 = QtWidgets.QWidget(self.centralwidget)
        self.horizontalLayoutWidget_2.setGeometry(QtCore.QRect(520, 110, 421, 481))
        self.horizontalLayoutWidget_2.setObjectName("horizontalLayoutWidget_2")
        self.horizontalLayout_2 = QtWidgets.QHBoxLayout(self.horizontalLayoutWidget_2)
        self.horizontalLayout_2.setContentsMargins(0, 0, 0, 0)
        self.horizontalLayout_2.setObjectName("horizontalLayout_2")
        self.textEdit = QtWidgets.QTextEdit(self.horizontalLayoutWidget_2)
        font = QtGui.QFont()
        font.setFamily("Adobe Devanagari")
        font.setPointSize(14)
        font.setBold(True)
        font.setWeight(75)
        self.textEdit.setFont(font)
        self.textEdit.setObjectName("textEdit")
        self.horizontalLayout_2.addWidget(self.textEdit)
        self.label_sent_title = QtWidgets.QLabel(self.centralwidget)
        self.label_sent_title.setGeometry(QtCore.QRect(650, 10, 171, 41))
        self.label_sent_title.setStyleSheet("font: 16pt \"华文琥珀\";")
        self.label_sent_title.setObjectName("label_sent_title")
        self.horizontalLayoutWidget = QtWidgets.QWidget(self.centralwidget)
        self.horizontalLayoutWidget.setGeometry(QtCore.QRect(10, 180, 461, 531))
        self.horizontalLayoutWidget.setObjectName("horizontalLayoutWidget")
        self.horizontalLayout_3 = QtWidgets.QHBoxLayout(self.horizontalLayoutWidget)
        self.horizontalLayout_3.setContentsMargins(0, 0, 0, 0)
        self.horizontalLayout_3.setObjectName("horizontalLayout_3")
        self.listView = QtWidgets.QListView(self.horizontalLayoutWidget)
        self.listView.setObjectName("listView")
        self.horizontalLayout_3.addWidget(self.listView)
        self.label = QtWidgets.QLabel(self.centralwidget)
        self.label.setGeometry(QtCore.QRect(610, 70, 241, 31))
        font = QtGui.QFont()
        font.setFamily("方正舒体")
        font.setPointSize(14)
        self.label.setFont(font)
        self.label.setObjectName("label")
        self.textEdit_customer = QtWidgets.QTextEdit(self.centralwidget)
        self.textEdit_customer.setGeometry(QtCore.QRect(130, 90, 251, 51))
        font = QtGui.QFont()
        font.setFamily("Bookman Old Style")
        font.setPointSize(18)
        font.setBold(True)
        font.setWeight(75)
        self.textEdit_customer.setFont(font)
        self.textEdit_customer.setObjectName("textEdit_customer")
        self.label_2 = QtWidgets.QLabel(self.centralwidget)
        self.label_2.setGeometry(QtCore.QRect(680, 600, 101, 31))
        font = QtGui.QFont()
        font.setFamily("方正舒体")
        font.setPointSize(14)
        self.label_2.setFont(font)
        self.label_2.setObjectName("label_2")
        self.textBrowser_score = QtWidgets.QTextBrowser(self.centralwidget)
        self.textBrowser_score.setGeometry(QtCore.QRect(520, 670, 181, 41))
        self.textBrowser_score.setObjectName("textBrowser_score")
        self.textBrowser_sent = QtWidgets.QTextBrowser(self.centralwidget)
        self.textBrowser_sent.setGeometry(QtCore.QRect(770, 670, 181, 41))
        font = QtGui.QFont()
        font.setFamily("楷体")
        font.setPointSize(18)
        self.textBrowser_sent.setFont(font)
        self.textBrowser_sent.setObjectName("textBrowser_sent")
        self.label_3 = QtWidgets.QLabel(self.centralwidget)
        self.label_3.setGeometry(QtCore.QRect(560, 630, 101, 31))
        font = QtGui.QFont()
        font.setFamily("方正舒体")
        font.setPointSize(14)
        self.label_3.setFont(font)
        self.label_3.setObjectName("label_3")
        self.label_4 = QtWidgets.QLabel(self.centralwidget)
        self.label_4.setGeometry(QtCore.QRect(810, 630, 101, 31))
        font = QtGui.QFont()
        font.setFamily("方正舒体")
        font.setPointSize(14)
        self.label_4.setFont(font)
        self.label_4.setObjectName("label_4")
        self.label_5 = QtWidgets.QLabel(self.centralwidget)
        self.label_5.setGeometry(QtCore.QRect(180, 150, 101, 31))
        font = QtGui.QFont()
        font.setFamily("方正舒体")
        font.setPointSize(14)
        self.label_5.setFont(font)
        self.label_5.setObjectName("label_5")
        self.recommend_button = QtWidgets.QPushButton(self.centralwidget)
        self.recommend_button.setGeometry(QtCore.QRect(400, 90, 71, 51))
        font = QtGui.QFont()
        font.setFamily("Adobe Devanagari")
        font.setPointSize(18)
        font.setBold(True)
        font.setItalic(True)
        font.setWeight(75)
        self.recommend_button.setFont(font)
        self.recommend_button.setObjectName("recommend_button")
        self.sentiment_button = QtWidgets.QPushButton(self.centralwidget)
        self.sentiment_button.setGeometry(QtCore.QRect(860, 70, 81, 41))
        font = QtGui.QFont()
        font.setFamily("Adobe Devanagari")
        font.setPointSize(18)
        font.setBold(True)
        font.setItalic(True)
        font.setWeight(75)
        self.sentiment_button.setFont(font)
        self.sentiment_button.setObjectName("sentiment_button")
        mainWindow.setCentralWidget(self.centralwidget)
        self.menubar = QtWidgets.QMenuBar(mainWindow)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 972, 26))
        self.menubar.setObjectName("menubar")
        mainWindow.setMenuBar(self.menubar)
        self.statusbar = QtWidgets.QStatusBar(mainWindow)
        self.statusbar.setObjectName("statusbar")
        mainWindow.setStatusBar(self.statusbar)

        self.retranslateUi(mainWindow)
        QtCore.QMetaObject.connectSlotsByName(mainWindow)

    def retranslateUi(self, mainWindow):
        _translate = QtCore.QCoreApplication.translate
        mainWindow.setWindowTitle(_translate("mainWindow", "大作业-冯郭晟"))
        self.label_recommend_title.setText(_translate("mainWindow", "协同推荐"))
        self.label_customer.setText(_translate("mainWindow", "顾客ID"))
        self.textEdit.setHtml(_translate("mainWindow",
                                         "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n"
                                         "<html><head><meta name=\"qrichtext\" content=\"1\" /><style type=\"text/css\">\n"
                                         "p, li { white-space: pre-wrap; }\n"
                                         "</style></head><body style=\" font-family:\'Adobe Devanagari\'; font-size:14pt; font-weight:600; font-style:normal;\">\n"
                                         "<p style=\"-qt-paragraph-type:empty; margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><br /></p></body></html>"))
        self.label_sent_title.setText(_translate("mainWindow", "情感走向预测"))
        self.label.setText(_translate("mainWindow", "请输入需要预测的评论"))
        self.textEdit_customer.setHtml(_translate("mainWindow",
                                                  "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n"
                                                  "<html><head><meta name=\"qrichtext\" content=\"1\" /><style type=\"text/css\">\n"
                                                  "p, li { white-space: pre-wrap; }\n"
                                                  "</style></head><body style=\" font-family:\'Bookman Old Style\'; font-size:18pt; font-weight:600; font-style:normal;\">\n"
                                                  "<p style=\"-qt-paragraph-type:empty; margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><br /></p></body></html>"))
        self.label_2.setText(_translate("mainWindow", " 分析结果"))
        self.textBrowser_sent.setHtml(_translate("mainWindow",
                                                 "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n"
                                                 "<html><head><meta name=\"qrichtext\" content=\"1\" /><style type=\"text/css\">\n"
                                                 "p, li { white-space: pre-wrap; }\n"
                                                 "</style></head><body style=\" font-family:\'楷体\'; font-size:18pt; font-weight:400; font-style:normal;\">\n"
                                                 "<p style=\"-qt-paragraph-type:empty; margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><br /></p></body></html>"))
        self.label_3.setText(_translate("mainWindow", "情感得分"))
        self.label_4.setText(_translate("mainWindow", "情感走向"))
        self.label_5.setText(_translate("mainWindow", " 推荐结果"))
        self.recommend_button.setText(_translate("mainWindow", "Go!"))
        self.sentiment_button.setText(_translate("mainWindow", "done!"))


class main_GUI(QtWidgets.QMainWindow, Ui_mainWindow):
    def __init__(self, parent=None):
        QtWidgets.QMainWindow.__init__(self, parent)
        self.setupUi(self)
        self.sentiment_button.clicked.connect(self.segment_predict)
        self.recommend_button.clicked.connect(self.recommend)

        self.msgBox = QtWidgets.QMessageBox()
        self.msgBox.setWindowTitle(u'提示')
        self.msgBox.setText("请输入内容!")

        self.positive_path = "./Segment/positive.csv"
        self.negative_path = "./Segment/negative.csv"

    def segment_predict(self):
        review_str = self.textEdit.toPlainText()
        if len(review_str) == 0:
            self.msgBox.show()
            return

        positive_df = pd.read_csv(self.positive_path)
        negative_df = pd.read_csv(self.negative_path)

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
        self.textBrowser_score.setPlainText(str(score))
        if score > 0:
            sent = "积极"
        elif score < 0:
            sent = "消极"
        else:
            sent = "一般般"
        self.textBrowser_sent.setPlainText(sent)

        return round(score, 3)

    def recommend(self):
        customer = self.textEdit_customer.toPlainText()
        if len(customer) == 0:
            self.msgBox.show()
            return
        customer = int(customer)

        self.textEdit_customer.setPlainText("please wait ...")

        """
            initiate the spark context
            """
        data_path = "./Yelp_Data"
        model_path = "F:/MyDeskTop/BigData_Analysis/Big_Homework/Project/Models/ASL_model"
        sc = SparkContext(master="local", appName="BigDataHomework")

        review_file_path = os.path.join(data_path, "yelp_academic_dataset_review10000.json")
        user_file_path = os.path.join(data_path, "yelp_academic_dataset_user10000.json")
        business_file_path = os.path.join(data_path, "yelp_academic_dataset_business10000.json")

        review_ids, user_ids, business_ids = get_recommend_profiles(sc, review_file_path, user_file_path,
                                                                    business_file_path)
        recommendModel = recommend_model(sc, review_ids, user_ids, business_ids)
        recommendModel.init_data()
        recommendModel.train_ASL_model(save_path=model_path)
        recommendModel.model_evaluation(MF_model_path=model_path)
        result = recommendModel.predict(model_path, customer, using_local=False)
        model = QtGui.QStandardItemModel()
        self.listView.setModel(model)
        for i in result:
            item = QtGui.QStandardItem(str("name: " + i[0] + ": " + i[1]))
            model.appendRow(item)


if __name__ == "__main__":
    import sys

    app = QtWidgets.QApplication(sys.argv)
    window = main_GUI()
    window.show()
    sys.exit(app.exec_())
