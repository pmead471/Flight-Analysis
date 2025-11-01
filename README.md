Flight Performance Analytics
End-to-end ETL pipeline analyzing 2+ million flight records from the Bureau of Transportation Statistics

Project Overview

This project analyzes historical flight data to identify performance patterns, delay trends, and operational bottlenecks across US airlines, routes, and cities. The pipeline transforms raw flight records into business-ready analytics tables for visualization in Tableau.

Project Architecture
CSV > PostgreSQL DB > Transformation > Tableau Visualization

Analytical Tables Created

1. `airline_yoy_performance`
Year-over-year comparison delay grouped by airline

2. `top_10_delayed_routes`
Most problematic origin-destination pairs with by delay

3. `departure_delay`
City-level departure performance metrics (50+ cities with >15,000 flights)

4. `arrival_delay`
City-level arrival performance metrics (same structure as departure_delay)

Pandas Transformations
Airline YoY Performance
<img width="739" height="106" alt="image" src="https://github.com/user-attachments/assets/f93181da-cac2-4bdc-9c7a-22e082966f84" />
1. Added pct change column
<img width="821" height="103" alt="image" src="https://github.com/user-attachments/assets/ebe52882-cb74-4157-817c-329d212a33e6" />
2. Added a trend column
<img width="890" height="105" alt="image" src="https://github.com/user-attachments/assets/0421318e-10af-47bc-b569-812a5a0044c9" />
3. Added a total flights column
<img width="996" height="103" alt="image" src="https://github.com/user-attachments/assets/e10275f6-6e90-4a7b-b428-25785503b903" />

Departure Delay
1. Added delay categories
2. Added a normalized minutes column
3. Added a performance score
4. Added rank columns

Arrival Delay
1. Added delay categories
2. Added a normalized minutes column
3. Added a performance score
4. Added rank columns

