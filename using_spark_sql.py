# In Python
from pyspark.sql import SparkSession
# Create a SparkSession
spark = (SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate())

# Path to data set
csv_file = "/opt/spark/src/data/departuredelays.csv"

# Read and create a temporary view
# Infer schema (note that for larger files you
# may want to specify the schema)
df = (spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(csv_file))
df.createOrReplaceTempView("us_delay_flights_tbl")

# Use SparkSQL to find all flights whose distance is greater than 1,000 miles:
spark.sql("""SELECT distance, origin, destination
FROM us_delay_flights_tbl WHERE distance > 1000
ORDER BY distance DESC""").show(10)

# Find all flights between San Francisco (SFO) and Chicago
# with at least a two-hour delay:
spark.sql("""SELECT date, delay, origin, destination
FROM us_delay_flights_tbl
WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
ORDER by delay DESC""").show(10)


# Label all US flights, regardless of origin and destination,
# with an indication of the delays they experienced: Very Long Delays (> 6 hours),
# Long Delays (2–6 hours), etc. We’ll add these human-readable labels in a new column
# called Flight_Delays:
spark.sql("""SELECT delay, origin, destination,
CASE
WHEN delay > 360 THEN 'Very Long Delays'
WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
WHEN delay = 0 THEN 'No Delays'
ELSE 'Early'
END AS Flight_Delays
FROM us_delay_flights_tbl
ORDER BY origin, delay DESC""").show(10)