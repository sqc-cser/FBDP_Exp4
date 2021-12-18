import numpy as np
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer# 处理类别变量
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import LinearSVC
from pyspark import SparkConf
import matplotlib.pyplot as plt
from sklearn import metrics
from imblearn.under_sampling import RandomUnderSampler
from imblearn.over_sampling import SMOTE


def under_sample():
    df = pd.read_csv('train_data.csv')
    train_y = df['is_default']
    train_x = df.drop(columns=['is_default'])
    rus = RandomUnderSampler(random_state=0)
    x_under,y_under = rus.fit_resample(train_x,train_y)
    df = pd.concat([x_under,y_under],axis=1)
    df.to_csv('train_data_under.csv')


def under_sample():
    feature_to_trans.append('is_default')
    df = df_feature.select(feature_to_trans).toPandas()
    train_y = df['is_default']
    train_x = df.drop(columns=['is_default'])
    rus = SMOTE(random_state=0)
    x_under,y_under = rus.fit_resample(train_x,train_y)
    df = pd.concat([x_under,y_under],axis=1)
    df.to_csv('train_data_smote.csv')


def evaluate_svm(testResult):
    TP = testResult.filter(testResult['prediction'] == 1).filter(testResult['is_default'] == 1).count()
    FN = testResult.filter(testResult['prediction'] == 0).filter(testResult['is_default'] == 1).count()
    TN = testResult.filter(testResult['prediction'] == 0).filter(testResult['is_default'] == 0).count()
    FP = testResult.filter(testResult['prediction'] == 1).filter(testResult['is_default'] == 0).count()
    # 计算准确率 （TP+TN)/(TP+TN+FP+FN)
    acc = (TP + TN) / (TP + TN + FP + FN)
    # 计算召回率 TP/（TP+TN）
    recall = TP / (TP + TN)
    print('计算准确率 acc = {}'.format(acc))
    print('计算召回率 recall = {}'.format(recall))


def evaluate(testResult):
    testResult = testResult.toPandas()
    y = list(testResult['is_default'])
    y_pred = list(testResult['prediction'])
    y_predprob = [x[1] for x in list(testResult['probability'])]
    precision_score = metrics.precision_score(y, y_pred)  # 精确率
    recall_score = metrics.recall_score(y, y_pred)  # 召回率
    accuracy_score = metrics.accuracy_score(y, y_pred)  # 准确率
    f1_score = metrics.f1_score(y, y_pred)  # F1分数
    auc_score = metrics.roc_auc_score(y, y_predprob)  # auc分数
    print("精确率:", precision_score)  # 精确率
    print("召回率:", recall_score)  # 召回率
    print("准确率:", accuracy_score)  # 准确率
    print("F1分数:", f1_score)  # F1分数
    print("auc分数:", auc_score)  # auc分数


if __name__ == '__main__':
    Appname = 'practice'
    master = "local[4]"
    Config = SparkConf().setAppName(Appname).setMaster(master)
    # 题目二，分区间输出频率
    spark = SparkSession.builder.config(conf=Config).getOrCreate()  # spark实例化
    # spark = SparkSession.builder.config().appName('Practice').getOrCreate()  # 创建session
    # 创建会话
    sc = SparkContext.getOrCreate()
    sc.setLogLevel("ERROR")
    df_spark = spark.read.option('header', 'true').csv('train_data.csv', inferSchema=True)  # 原数据
    # df_spark = spark.read.option('header', 'true').csv('train_data_smote.csv', inferSchema=True)  # smote采样后数据
    # df_spark = spark.read.option('header', 'true').csv('train_data_under.csv', inferSchema=True)  # 欠采样后数据
    
    df_loan = df_spark.select('total_loan')
    pandas_loan = df_loan.toPandas()
    # 方法1 binning
    rdd = sc.parallelize(pandas_loan['total_loan'].tolist()).histogram(
        list(range(0, (int(pandas_loan.max() / 1000) + 2) * 1000, 1000)))
    result = []
    for key in range(len(rdd[0]) -1 ):
        result.append(f'(({rdd[0][key]},{rdd[0][key+1]}),{rdd[1][key]})')
    np.savetxt("result_2.txt",result, fmt="%s", delimiter="\n")

    # 题目三，Spark SQL 对⽹网络信⽤用贷产品记录数据进⾏行行如下统计
    # 1.统计所有⽤用户所在公司类型 employer_type 的数量量分布占⽐比情况。
    # 输出成 CSV 格式的⽂文件，输出内容格式为: <公司类型>,<类型占⽐比>
    df_employer = df_spark.select('employer_type')
    pandas_employer = df_employer.toPandas()
    all_employer = list(set(pandas_employer['employer_type'].tolist()))
    all_employer.sort()
    # 方法1 binning （错误）
    # rdd = sc.parallelize(pandas_employer['employer_type'].tolist()).histogram(
    #     all_employer)
    # result = []
    # sum = df_employer.count()
    # for key in range(len(rdd[0]) - 1):
    #     result.append(f'{rdd[0][key]}, {rdd[1][key]/sum}')
    # 方法2 wordcount
    rdd = sc.parallelize(pandas_employer['employer_type'].tolist())
    result = rdd.map(lambda x:(x,1)).reduceByKey(lambda a, b: a + b)
    result = result.collect()
    res = [];sum = df_employer.count()
    for i in range(len(result)):
        res.append(f"{result[i][0]}, {result[i][1]/sum}")
    np.savetxt("result_3_1.csv", res, fmt="%s", delimiter="\n")
    # 2.统计每个⽤用户最终须缴纳的利利息⾦金金额
    # 输出成CSV格式的⽂件，输出内容格式为: < user_id >, < total_money >
    df_interest = df_spark.select(['user_id','year_of_loan','monthly_payment','total_loan'])
    df_interest = df_interest.withColumn("total_money",
                          df_interest['year_of_loan'] * 12 * df_interest['monthly_payment']-df_interest['total_loan'])
    # 存储
    pandas_interest = df_interest.select(['user_id', 'total_money']).toPandas()
    pandas_interest.to_csv('result_3_2.csv',index=0)

    # 3.统计⼯工作年年限 work_year 超过 5 年年的⽤用户的房贷情况 censor_status 的数量量分布占⽐比情况。
    # 输出成 CSV 格式的⽂文件，输出内容格式为:<user_id>,<censor_status>,<work_year>
    df_wk = df_spark.select(['user_id', 'censor_status', 'work_year'])
    df_wk_f = df_wk.filter(((df_wk['work_year'] > '5 years') | (df_wk['work_year'] == '10+ years'))
                           & ~(df_wk['work_year'] == '< 1 year'))
    # 存储
    df_wk_f.toPandas().to_csv('result_3_3.csv',index=0)
    
    # 题目四
    # 将类别变量变化
    # 随机森林方法: https://blog.csdn.net/weixin_43790705/article/details/108653416
    indexer = StringIndexer(inputCols=['class', 'sub_class', 'work_type', 'employer_type','industry','work_year'],
                            outputCols=['class_index', 'sub_class_index', 'work_type_index', 'employer_type_index', 'industry_index','work_year_index'],
                            handleInvalid='skip')
    df_r = indexer.fit(df_spark).transform(df_spark)

    feature_to_trans = ['total_loan', 'year_of_loan', 'interest', 'monthly_payment', 'class_index',
                        'sub_class_index', 'work_type_index', 'employer_type_index', 'industry_index','work_year_index',
                        'house_exist', 'house_loan_status', 'censor_status', 'marriage','offsprings','use', 'post_code','region','debt_loan_ratio',
                        'del_in_18month','scoring_low','scoring_high','pub_dero_bankrup','early_return','early_return_amount','early_return_amount_3mon',
                        'recircle_b','recircle_u','initial_list_status','title','policy_code','f0','f1','f2','f3','f4','f5']
    # 根据 模型特征重要性 删减特征
    # feature_to_trans = ['interest', 'work_type_index', 'employer_type_index', 'marriage','offsprings']
    feature = VectorAssembler(inputCols=feature_to_trans,outputCol='Independent Features', handleInvalid='skip')
    df_feature = feature.transform(df_r)
    final = df_feature.select(['Independent Features', 'is_default'])
    train, test = final.randomSplit([0.8, 0.2])
    #  随机森林回归

    rfModel = RandomForestClassifier(featuresCol="Independent Features", labelCol='is_default',
                                     maxBins=100, maxDepth=10, numTrees=20).fit(train)
    testResult = rfModel.transform(test)
    evaluate(testResult)
    # 输出模型特征重要性、子树权重
    print("模型特征重要性:{}".format(rfModel.featureImportances))
    print("模型特征数:{}".format(rfModel.numFeatures))
    plt.plot(feature_to_trans, rfModel.featureImportances)
    plt.show()

    # SVM 方法
    # svm = LinearSVC(featuresCol="Independent Features", labelCol="is_default").fit(train)
    # testResult = svm.transform(test)
    # evaluate_svm(testResult)


