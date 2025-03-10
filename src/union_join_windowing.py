# Set file paths
from pyspark.sql.functions import expr
tripdelaysFilePath = "/opt/spark/src/data/flights/departuredelays.csv"
airportsnaFilePath = "/opt/spark/src/data/flights/airport-codes-na.txt"
# Obtain airports data set
airportsna = (spark.read
    .format("csv")
    .options(header="true", inferSchema="true", sep="\t")
    .load(airportsnaFilePath))
airportsna.createOrReplaceTempView("airports_na")
# Obtain departure delays data set
departureDelays = (spark.read
    .format("csv")
    .options(header="true")
    .load(tripdelaysFilePath))
departureDelays = (departureDelays
    .withColumn("delay", expr("CAST(delay as INT) as delay"))
    .withColumn("distance", expr("CAST(distance as INT) as distance")))
departureDelays.createOrReplaceTempView("departureDelays")
# Create temporary small table
foo = (departureDelays
    .filter(expr("""origin == 'SEA' and destination == 'SFO' and
    date like '01010%' and delay > 0""")))
foo.createOrReplaceTempView("foo")

# Union dataframes
bar = departureDelays.union(foo)
bar.createOrReplaceTempView("bar")
# Show the union (filtering for SEA and SFO in a specific time range)
bar.filter(expr("""origin == 'SEA' AND destination == 'SFO'
AND date LIKE '01010%' AND delay > 0""")).show()


# Join departure delays data (foo) with airport info
foo.join(
    airportsna,
    airportsna.IATA == foo.origin
    ).select("City", "State", "date", "delay", "distance", "destination").show()

# Window function
# Find the three destinations thatexperienced the most delays
spark.sql("""
            DROP TABLE IF EXISTS departureDelaysWindow
          """)
spark.sql("""
          CREATE TABLE departureDelaysWindow AS
SELECT origin, destination, SUM(delay) AS TotalDelays
FROM departureDelays
WHERE origin IN ('SEA', 'SFO', 'JFK')
AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')
GROUP BY origin, destination;
          """)
spark.sql("""
SELECT origin, destination, TotalDelays, rank
FROM (
SELECT origin, destination, TotalDelays, dense_rank()
OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank
FROM departureDelaysWindow
) t
WHERE rank <= 3
""").show()