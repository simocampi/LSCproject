def split_train_test(all_data):
    train_data, test_data = scaled_df.randomSplit([.8,.2],seed=1234)

    return train_data, test_data


def divide_in_label_and_feature(rdd):
    label_pos = 8               #DA AGGIUSTARE

    input_data = rdd.map(lambda x: (x[0], DenseVector(x[1:])))

    df = spark.createDataFrame(input_data, ["label", "features"])


def OneHotEncoder(rdd):
    pass