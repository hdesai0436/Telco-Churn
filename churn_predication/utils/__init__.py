from pyspark.sql import *
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import os
import sys

def get_score(dataframe: DataFrame, metric_name, label_col, prediction_col) -> float:
    try:
        evaluator = MulticlassClassificationEvaluator(
            labelCol=label_col, predictionCol=prediction_col,
            metricName=metric_name)
        score = evaluator.evaluate(dataframe)
        print(f"{metric_name} score: {score}")
        #logging.info(f"{metric_name} score: {score}")
        return score
    except Exception as e:
        raise e