# PySpark_with_Python
![logo](https://github.com/prabhakarvenkat/PySpark_with_Python/blob/67ffc0437ac8f204d6bca7441059db6661920697/PySpark.jpg)

Welcome to the **PySpark Tutorials** repository! This repository provides an in-depth understanding of PySpark, a powerful big data processing framework, through hands-on Python examples. Whether you're a beginner or looking to expand your knowledge, this guide covers essential PySpark concepts.

## Table of Contents

1. [Introduction to PySpark](#introduction-to-pyspark)
2. [DataFrames in PySpark](#dataframes-in-pyspark)
3. [Handling Missing Values](#handling-missing-values)
4. [Filter Operations](#filter-operations)
5. [GroupBy and Aggregate Functions](#groupby-and-aggregate-functions)
6. [Introduction to PySpark MLlib](#introduction-to-pyspark-mllib)
7. [How to Run the Code](#how-to-run-the-code)
8. [Resources and References](#resources-and-references)

---

### 1. Introduction to PySpark

PySpark is the Python API for Apache Spark, enabling scalable and distributed data processing. It allows developers to process large datasets efficiently using RDDs (Resilient Distributed Datasets) or DataFrames.

Key Features:
- Distributed computing across clusters
- High-level APIs for big data processing
- Integration with MLlib for machine learning

Example:
```python
from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Intro to PySpark") \
    .getOrCreate()

# Sample code to print Spark version
print(f"Spark Version: {spark.version}")
```

---

### 2. DataFrames in PySpark

A PySpark DataFrame is a distributed collection of data organized into named columns, similar to a table in a database.

Features:
- Schema enforcement
- High-level APIs for querying
- Optimized performance through Catalyst optimizer

Example:
```python
# Create a DataFrame
data = [("Alice", 25), ("Bob", 30), ("Cathy", 28)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()
```

Output:
```
+-----+---+
| Name|Age|
+-----+---+
|Alice| 25|
|  Bob| 30|
|Cathy| 28|
+-----+---+
```

---

### 3. Handling Missing Values

PySpark provides methods to handle missing values in a dataset, such as `fillna`, `dropna`, and `replace`.

Example:
```python
data = [("Alice", 25, None), ("Bob", None, "HR"), ("Cathy", 28, None)]
columns = ["Name", "Age", "Department"]

df = spark.createDataFrame(data, columns)

# Fill missing values
df_filled = df.fillna({"Age": 30, "Department": "Unknown"})
df_filled.show()
```

Output:
```
+-----+---+----------+
| Name|Age|Department|
+-----+---+----------+
|Alice| 25|     HR   |
|  Bob| 30|  Unknown |
|Cathy| 28|  Unknown |
+-----+---+----------+
```

---

### 4. Filter Operations

PySpark allows filtering of rows based on conditions using the `filter` or `where` methods.

Example:
```python
# Filter rows where Age > 26
filtered_df = df.filter(df.Age > 26)
filtered_df.show()
```

Output:
```
+-----+---+
| Name|Age|
+-----+---+
|  Bob| 30|
|Cathy| 28|
+-----+---+
```

---

### 5. GroupBy and Aggregate Functions

PySpark supports grouping data and applying aggregate functions like `sum`, `avg`, `count`, etc.

Example:
```python
data = [("HR", 5000), ("IT", 7000), ("HR", 4000), ("IT", 8000)]
columns = ["Department", "Salary"]

df = spark.createDataFrame(data, columns)

# Group by Department and calculate average Salary
df_grouped = df.groupBy("Department").avg("Salary")
df_grouped.show()
```

Output:
```
+----------+-----------+
|Department|avg(Salary)|
+----------+-----------+
|        HR|     4500.0|
|        IT|     7500.0|
+----------+-----------+
```

---

### 6. Introduction to PySpark MLlib

MLlib is PySpark's machine learning library, offering tools for:
- Classification
- Regression
- Clustering
- Feature engineering

Example: Linear Regression
```python
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler

data = [(1.0, 2.0, 3.0, 4.0),
        (2.0, 3.0, 4.0, 5.0),
        (3.0, 4.0, 5.0, 6.0)]
columns = ["feature1", "feature2", "feature3", "label"]

df = spark.createDataFrame(data, columns)

# Feature Engineering
assembler = VectorAssembler(inputCols=["feature1", "feature2", "feature3"], outputCol="features")
transformed_df = assembler.transform(df)

# Train a Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="label")
model = lr.fit(transformed_df)

# Print coefficients and intercept
print(f"Coefficients: {model.coefficients}")
print(f"Intercept: {model.intercept}")
```

---

### 7. How to Run the Code

1. Install Apache Spark and PySpark.
2. Clone this repository:
   ```bash
   git clone https://github.com/prabhakarvenkat/PySpark_with_Python.git
   ```
3. Navigate to the repository:
   ```bash
   cd Intro_to_PySpark
   ```
4. Run Python scripts using jupyter notebooks

---

### 8. Resources and References

- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Apache Spark Website](https://spark.apache.org/)
- [MLlib Guide](https://spark.apache.org/mllib/)

---

### Contributions

Contributions are welcome! Feel free to fork the repo, make changes, and submit a pull request.

---

### License

This repository is licensed under the Apache License. See the `LICENSE` file for details.

---
