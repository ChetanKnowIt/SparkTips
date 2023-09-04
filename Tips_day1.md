# Shuffling in Spark

Using appropriate partitioning
### Sample data
```
data = [(1, "A"), (2, "B"), (3, "C"), (4, "D"), (5, "E")]
```
### Create a DataFrame
```
df = spark.createDataFrame(data, ["id", "name"])
```
### Bad - Shuffling involved due to default partitioning (200 partitions)
```
result_bad = df.groupBy("id").count()
```
### Good - Avoids shuffling by explicitly repartitioning (2 partitions)
```
df_repartitioned = df.repartition(2, "id")
result_good = df_repartitioned.groupBy("id").count()
```


### Sample data
```
sales_data = [(101, "Product A", 100), (102, "Product B", 150), (103, "Product C", 200)]
categories_data = [(101, "Category X"), (102, "Category Y"), (103, "Category Z")]
```

### Create DataFrames
```
sales_df = spark.createDataFrame(sales_data, ["product_id", "product_name", "price"])
categories_df = spark.createDataFrame(categories_data, ["product_id", "category"])
```

### Bad - Shuffling involved due to regular join
```
result_bad = sales_df.join(categories_df, on="product_id")
```

### Good - Avoids shuffling using broadcast variable
### Filter the small DataFrame early and broadcast it for efficient join
```
filtered_categories_df = categories_df.filter("category = 'Category X'")
result_good = sales_df.join(broadcast(filtered_categories_df), on="product_id")
```

### Sample data
```
products_data = [(101, "Product A", 100), (102, "Product B", 150), (103, "Product C", 200)]
categories_data = [(101, "Category X"), (102, "Category Y"), (103, "Category Z")]
```
### Create DataFrames
```
products_df = spark.createDataFrame(products_data, ["product_id", "product_name", "price"])
categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])
```
### Bad - Shuffling involved due to regular join
```
result_bad = products_df.join(categories_df, products_df.product_id == categories_df.category_id)
```
### Good - Avoids shuffling using broadcast variable
### Create a broadcast variable from the categories DataFrame
```
broadcast_categories = broadcast(categories_df)
```

### Join the DataFrames using the broadcast variable
```
result_good = products_df.join(broadcast_categories, products_df.product_id == broadcast_categories.category_id)
```

### Sample data
```
data = [(1, "click"), (2, "like"), (1, "share"), (3, "click"), (2, "share")]
```
### Create an RDD
```
rdd = sc.parallelize(data)
```
### Bad - Shuffling involved due to groupByKey
```
result_bad = rdd.groupByKey().mapValues(len)
```
### Good - Avoids shuffling by using reduceByKey
```
result_good = rdd.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)
```

### Sample data
```
data = [(1, 10), (2, 20), (1, 5), (3, 15), (2, 25)]
```
### Create a DataFrame
```
df = spark.createDataFrame(data, ["key", "value"])
```
### Bad - Shuffling involved due to default data locality
```
result_bad = df.groupBy("key").max("value")
```

### Good - Avoids shuffling by repartitioning and using data locality
```
df_repartitioned = df.repartition("key")  # Repartition to align data by key
result_good = df_repartitioned.groupBy("key").max("value")
```

### Sample data
```
data = [(1, 10), (2, 20), (1, 5), (3, 15), (2, 25)]
```
### Create a DataFrame
```
df = spark.createDataFrame(data, ["key", "value"])
```

### Bad - Shuffling involved due to recomputation of the filter condition
```
result_bad = df.filter("value > 10").groupBy("key").sum("value")
```
### Good - Avoids shuffling by caching the filtered data
```
df_filtered = df.filter("value > 10").cache()
result_good = df_filtered.groupBy("key").sum("value")
```

### Create a Spark session with KryoSerializer to reduce data size during shuffling
```
spark = SparkSession.builder \
    .appName("AvoidShuffleExample") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()
```