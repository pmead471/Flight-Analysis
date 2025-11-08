# Flight Performance Analytics

End-to-end ETL pipeline analyzing flight records from the Bureau of Transportation Statistics

Project Overview

This project analyzes historical flight data to identify performance patterns, delay trends, and operational bottlenecks across US airlines, routes, and cities. The pipeline transforms raw flight records into business-ready analytics tables for visualization in Tableau.

Project Architecture
1. CSV (Download from BTS)
2. PostgreSQL DB Creation (flight_analysis_schema)
3. Data Load (flight_analysis_table_creation.py)
4. Transformation (load_bts_data.py)
5. Tableau Visualization

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

<img width="448" height="107" alt="image" src="https://github.com/user-attachments/assets/8f8faae1-ffca-4a94-97e9-b1354008b2b7" />

1. Added delay categories
<img width="551" height="103" alt="image" src="https://github.com/user-attachments/assets/447158c0-b8a1-4b96-87d7-8e59e0115adf" />

2. Added a normalized minutes/taxi column
<img width="857" height="101" alt="image" src="https://github.com/user-attachments/assets/1328c4b4-7b67-4671-8dfa-a46dc16634af" />

3. Added a performance score
<img width="985" height="103" alt="image" src="https://github.com/user-attachments/assets/9961a440-4812-42e4-939c-cf6b294e5bf4" />

4. Added rank columns
<img width="1283" height="105" alt="image" src="https://github.com/user-attachments/assets/3f858929-ac33-4be3-95af-38542234a064" />

Arrival Delay

<img width="551" height="100" alt="image" src="https://github.com/user-attachments/assets/1f52d14d-3b6a-45b9-8e2d-905c6a4ff340" />

1. Added delay categories
<img width="648" height="103" alt="image" src="https://github.com/user-attachments/assets/5582eadb-9ca5-43a8-9b6f-17cf696d3a3f" />

2. Added a normalized minutes/taxi column
<img width="945" height="100" alt="image" src="https://github.com/user-attachments/assets/c5363f7a-46f8-47a2-b78a-6b08f4d3bd13" />

3. Added a performance score
<img width="1088" height="105" alt="image" src="https://github.com/user-attachments/assets/102fbc49-1f9f-4d82-93a1-5363884841dc" />

4. Added rank columns
<img width="1380" height="105" alt="image" src="https://github.com/user-attachments/assets/947073f2-552b-4036-af47-84c912e84808" />

Tableau Analysis
With the above queries defined as a data source for tableau, the following dashboard was created:
https://public.tableau.com/app/profile/patrick.mead/vizzes

