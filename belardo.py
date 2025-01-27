# This is the unified Source Code initially written in Google Collab

from pyspark import SparkContext

# Initialize Spark Context
sc = SparkContext("local", "RDD Example")

# Sample dataset: Grades of 20 students
grades = [90, 87, 99, 98, 97, 90, 95, 99, 90, 100, 92, 93, 94, 78, 79, 86, 89, 90, 99, 100]

# Parallelize the dataset into an RDD
rdd = sc.parallelize(grades)

# 1. map(): Increment each grade by 5
incremented_grades = rdd.map(lambda x: x + 5)
print("Incremented Grades:", incremented_grades.collect())

# 2. reduce(): Calculate the sum of grades
total_sum = rdd.reduce(lambda a, b: a + b)
average_grade = total_sum / rdd.count()
print("Total Sum:", total_sum)
print("Average Grade:", average_grade)

# 3. filter(): Filter grades greater than or equal to 90
high_scores = rdd.filter(lambda x: x >= 90)
print("High Scores:", high_scores.collect())

# 4. distinct(): Find unique grades
unique_grades = rdd.distinct()
print("Unique Grades:", unique_grades.collect())

# 5. sortBy(): Sort grades in ascending order
sorted_grades = rdd.sortBy(lambda x: x, ascending=True)
print("Sorted Grades:", sorted_grades.collect())

# Stop Spark Context
sc.stop()
