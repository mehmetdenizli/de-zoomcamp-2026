import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:postgres@localhost:5432/ny_taxi')

# Q3: Counting short trips
query3 = """
SELECT count(1) 
FROM green_tripdata 
WHERE lpep_pickup_datetime >= '2025-11-01' AND lpep_pickup_datetime < '2025-12-01'
AND trip_distance <= 1;
"""
print("Answer 3:", pd.read_sql(query3, engine).iloc[0,0])

# Q4: Longest trip for each day
query4 = """
SELECT CAST(lpep_pickup_datetime AS DATE), MAX(trip_distance)
FROM green_tripdata
WHERE trip_distance < 100
GROUP BY 1 ORDER BY 2 DESC LIMIT 1;
"""
print("Answer 4:\n", pd.read_sql(query4, engine))

# Q5: Biggest pickup zone
query5 = """
SELECT z."Zone", SUM(t.total_amount)
FROM green_tripdata t JOIN zones z ON t."PULocationID" = z."LocationID"
WHERE CAST(t.lpep_pickup_datetime AS DATE) = '2025-11-18'
GROUP BY 1 ORDER BY 2 DESC LIMIT 1;
"""
print("Answer 5:\n", pd.read_sql(query5, engine))

# Q6: Largest tip
query6 = """
SELECT zdo."Zone", MAX(t.tip_amount)
FROM green_tripdata t
JOIN zones zpu ON t."PULocationID" = zpu."LocationID"
JOIN zones zdo ON t."DOLocationID" = zdo."LocationID"
WHERE zpu."Zone" = 'East Harlem North'
AND t.lpep_pickup_datetime >= '2025-11-01' AND t.lpep_pickup_datetime < '2025-12-01'
GROUP BY 1 ORDER BY 2 DESC LIMIT 1;
"""
print("Answer 6:\n", pd.read_sql(query6, engine))