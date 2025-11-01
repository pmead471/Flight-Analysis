import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:PASSWORD@localhost:5432/bts_flights')

# Your query
query = """
SELECT 
    a.airline_name as airline,
    ROUND(AVG(CASE WHEN year = 2021 THEN f.arr_delay END), 2) as avg_delay_2021,
    ROUND(AVG(CASE WHEN year = 2022 THEN f.arr_delay END), 2) as avg_delay_2022,
    ROUND(AVG(CASE WHEN year = 2022 THEN f.arr_delay END) - 
          AVG(CASE WHEN year = 2021 THEN f.arr_delay END), 2) as change_2021_to_2022,
    COUNT(CASE WHEN year = 2021 THEN 1 END) as flights_2021,
    COUNT(CASE WHEN year = 2022 THEN 1 END) as flights_2022
FROM flights f
JOIN airlines a on a.carrier_code = f.carrier_code
WHERE year IN (2021, 2022)
  AND cancelled = 0
GROUP BY a.airline_name
ORDER BY change_2021_to_2022
"""

# Load into pandas
df = pd.read_sql(query, engine)

# Add pct change column
df['pct_change'] = ((df['avg_delay_2022'] - df['avg_delay_2021']) / 
                     df['avg_delay_2021'] * 100).round(2)
# Add trend column
df['trend'] = df['change_2021_to_2022'].apply(
    lambda x: 'Improved' if x < 0 else 'Declined' if x > 0 else 'Same'
)
# Add total flights column
df['total_flights'] = df['flights_2021'] + df['flights_2022']

# Write to PostgreSQL as analysis table
df.to_sql('airline_yoy_performance', engine, if_exists='replace', index=False)

# create the top 10 delayed routes table
query1 = """
with cte as(
select count(*) as count, origin_airport, dest_airport, avg(arr_delay) as average
from flights
group by origin_airport, dest_airport
Order By avg(arr_delay)
)
Select * from cte
where cte.count > 2500
Order By cte.average desc
limit 10
"""

df1 = pd.read_sql(query1, engine)
# Write to postgreSQL as delayed routes table
df1.to_sql('top_10_delayed_routes', engine, if_exists='replace', index=False)

# create the delay table
query2 = """
with cte as(
select count(*) as count,a.city_name as city_name, round(avg(f.dep_delay_minutes),2) as delay_minutes, round(avg(f.taxi_out),2) as taxi, round(avg(f.cancelled)*100,2) as percent_cancelled
from flights f
join airports a on f.origin_airport = a.airport_code
Group by a.city_name
)
select cte.city_name, cte.delay_minutes, cte.taxi, cte.percent_cancelled
from cte
where cte.count > 15000
Order By cte.delay_minutes desc
"""

df2 = pd.read_sql(query2, engine)

#delay categories
df2['delay_category'] = pd.cut(df2['delay_minutes'],bins=[-float('inf'),0,10,20, float('inf')],labels=['Early', 'Minor','Moderate', 'Major'])

#normalize metrics
df2['delay_minutes_normalized'] = (df2['delay_minutes'] - df2['delay_minutes'].mean()) / df2['delay_minutes'].std()
df2['taxi_normalized'] = (df2['taxi'] - df2['taxi'].mean()) / df2['taxi'].std()

# Create a composite performance score
df2['performance_score'] = (
    df2['delay_minutes_normalized'] + 
    df2['taxi_normalized'] + 
    (df2['percent_cancelled'] / 100)
)

# rank by columns
df2['delay_rank'] = df2['delay_minutes'].rank(ascending=False)
df2['cancellation_rank'] = df2['percent_cancelled'].rank(ascending=False)
df2['taxi_rank'] = df2['taxi'].rank(ascending=False)

# Write to postgreSQL as departure delay table
df2.to_sql('departure_delay', engine, if_exists='replace', index=False)

# create the arrival table
query3 = """
with cte as(
select count(*) as count,a.city_name as city_name, round(avg(f.arr_delay_minutes),2) as delay_minutes, round(avg(f.taxi_in),2) as taxi, round(avg(f.cancelled)*100,2) as percent_cancelled
from flights f
join airports a on f.dest_airport = a.airport_code
Group by a.city_name
)
select cte.city_name, cte.delay_minutes, cte.taxi, cte.percent_cancelled
from cte
where cte.count > 15000
Order By cte.delay_minutes desc
"""

df3 = pd.read_sql(query3, engine)

#delay categories
df3['delay_category'] = pd.cut(df3['delay_minutes'],bins=[-float('inf'),0,10,20, float('inf')],labels=['Early', 'Minor','Moderate', 'Major'])

#normalize metrics
df3['delay_minutes_normalized'] = (df3['delay_minutes'] - df3['delay_minutes'].mean()) / df3['delay_minutes'].std()
df3['taxi_normalized'] = (df3['taxi'] - df3['taxi'].mean()) / df3['taxi'].std()

# Create a composite performance score
df3['performance_score'] = (
    df3['delay_minutes_normalized'] + 
    df3['taxi_normalized'] + 
    (df3['percent_cancelled'] / 100)
)

# rank columns
df3['delay_rank'] = df3['delay_minutes'].rank(ascending=False)
df3['cancellation_rank'] = df3['percent_cancelled'].rank(ascending=False)
df3['taxi_rank'] = df3['taxi'].rank(ascending=False)

# Write to postgreSQL as arrival delay table
df3.to_sql('arrival_delay', engine, if_exists='replace', index=False)
