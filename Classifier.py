from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint
from wav_manipulation.wav import *

class RandomForest():

    def __init__(self):
        self.prova = 2