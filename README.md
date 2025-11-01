# ✈️ Flight Performance Analytics Pipeline

> End-to-end ETL pipeline analyzing 2+ million flight records from the Bureau of Transportation Statistics (BTS)

## 📊 Project Overview

This project analyzes historical flight data to identify performance patterns, delay trends, and operational bottlenecks across US airlines, routes, and cities. The pipeline transforms raw flight records into business-ready analytics tables for visualization in Tableau.

### Key Insights Delivered
- 📈 Year-over-year airline performance trends (2021-2022)
- 🛫 Top 10 most delayed flight routes nationwide
- 🏙️ City-level departure and arrival performance rankings
- 🎯 Composite performance scoring system for fair comparison

---

## 🏗️ Project Architecture

```
┌─────────────────┐
│   Raw CSV Data  │
│   (BTS Files)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  PostgreSQL DB  │  ← Schema Design (schema.sql)
│  (bts_flights)  │  ← Data Loading (load_data.py)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Transformation  │  ← Analytics Layer (btstransform.py)
│  4 Analytical   │
│     Tables      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Tableau         │  ← Visualization & Dashboards
│ Dashboards      │
└─────────────────┘
```

---

## 🗂️ Project Structure

```
flight-analytics-pipeline/
├── README.md                      # This file
├── sql/
│   ├── schema.sql                 # Database schema definition
│   └── sample_queries.sql         # Example analysis queries
├── python/
│   ├── load_data.py              # Initial data ingestion
│   └── btstransform.py           # Analytics transformation pipeline
├── tableau/
│   └── dashboard.twb             # Tableau workbook (optional)
├── data/
│   └── sample_outputs/           # Sample CSV exports
└── docs/
    └── data_dictionary.md        # Column definitions
```

---

## 🛠️ Tech Stack

| Component | Technology |
|-----------|-----------|
| **Database** | PostgreSQL 13+ |
| **ETL & Analysis** | Python 3.8+, pandas, SQLAlchemy |
| **Visualization** | Tableau Desktop/Public |
| **Data Source** | Bureau of Transportation Statistics (BTS) |

---

## 📋 Analytical Tables Created

### 1. `airline_yoy_performance`
Year-over-year comparison of airline delay performance

**Columns:**
- `airline` - Airline name
- `avg_delay_2021` - Average arrival delay in 2021 (minutes)
- `avg_delay_2022` - Average arrival delay in 2022 (minutes)
- `change_2021_to_2022` - Absolute change in delay
- `pct_change` - Percentage change
- `trend` - Performance trend (Improved/Declined/Stable)
- `flights_2021`, `flights_2022`, `total_flights` - Flight counts

**Business Use:** Identify airlines with improving/declining performance

---

### 2. `top_10_delayed_routes`
Most problematic origin-destination pairs with high delays

**Columns:**
- `origin_airport` - Departure airport code
- `dest_airport` - Arrival airport code
- `flight_count` - Total flights on route (min 2,500)
- `avg_delay` - Average arrival delay (minutes)

**Business Use:** Target operational improvements on high-impact routes

---

### 3. `departure_delay`
City-level departure performance metrics (50+ cities with >15,000 flights)

**Columns:**
- `city_name` - Departure city
- `delay_minutes` - Average departure delay
- `taxi` - Average taxi-out time
- `percent_cancelled` - Cancellation rate (%)
- `delay_category` - Severity (Early/On-time, Minor, Moderate, Major)
- `delay_minutes_normalized` - Z-score normalized delay
- `taxi_normalized` - Z-score normalized taxi time
- `performance_score` - Composite score (lower = better)
- `delay_rank` - Ranking by delay (1 = worst)
- `cancellation_rank` - Ranking by cancellations
- `taxi_rank` - Ranking by taxi time

**Business Use:** Compare cities fairly across multiple performance dimensions

---

### 4. `arrival_delay`
City-level arrival performance metrics (same structure as departure_delay)

**Columns:** *(Same as departure_delay table)*

**Business Use:** Identify cities with arrival congestion issues

---

## 🚀 Getting Started

### Prerequisites
```bash
# Required software
- PostgreSQL 13+
- Python 3.8+
- Tableau Desktop/Public (for visualization)

# Python packages
pip install pandas sqlalchemy psycopg2-binary
```

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/flight-analytics-pipeline.git
cd flight-analytics-pipeline
```

2. **Set up PostgreSQL database**
```bash
# Create database
createdb bts_flights

# Run schema
psql -d bts_flights -f sql/schema.sql
```

3. **Load data**
```bash
# Update connection string in load_data.py
python python/load_data.py
```

4. **Run transformations**
```bash
# Update connection string in btstransform.py
python python/btstransform.py
```

5. **Connect Tableau**
- Open Tableau Desktop
- Connect to PostgreSQL Server
- Select the 4 analytical tables created

---

## 📈 Sample Insights

### Airline Performance Trends
- **Best Improvement:** [Airline X] reduced delays by 15% YoY
- **Worst Decline:** [Airline Y] delays increased by 23% YoY

### Route Analysis
- Top 10 delayed routes account for 8% of total delay minutes
- [Origin → Destination] has highest average delay at 47 minutes

### City Performance
- **Best Departure City:** [City A] (performance_score: -1.2)
- **Worst Departure City:** [City B] (performance_score: 2.4)
- Average cancellation rate: 1.8%

---

## 🔍 Key Methodology

### Performance Scoring System
The composite performance score normalizes different metrics to enable fair comparison:

```python
performance_score = delay_normalized + taxi_normalized + (cancellation_rate / 100)
```

- **Normalized metrics** use z-score standardization (mean=0, std=1)
- **Lower scores** indicate better performance
- Combines delay, taxi time, and cancellation rate equally

### Data Quality
- **Flight Volume Threshold:** Only cities with >15,000 flights included
- **Route Threshold:** Only routes with >2,500 flights analyzed
- **Exclusions:** Cancelled flights excluded from delay calculations

---

## 📊 Tableau Dashboard Features

### Suggested Visualizations
1. **Airline Trends** - Line chart showing YoY delay changes
2. **Route Heatmap** - Geographic map of most delayed routes
3. **City Rankings** - Bar charts of performance scores
4. **Delay Distribution** - Histogram of delay categories
5. **Correlation Matrix** - Relationship between delay, taxi, cancellation

---

## 📝 Data Dictionary

See [docs/data_dictionary.md](docs/data_dictionary.md) for detailed column definitions.

---

## 🎯 Future Enhancements

- [ ] Add weather data correlation analysis
- [ ] Implement time-series forecasting for delays
- [ ] Add day-of-week and seasonal patterns
- [ ] Create automated data quality monitoring
- [ ] Build real-time dashboard with API integration

---

## 🤝 Contributing

This is a portfolio project, but suggestions are welcome! Feel free to:
1. Fork the repository
2. Create a feature branch
3. Submit a pull request

---

## 📄 License

This project is open source and available under the MIT License.

---

## 👤 Contact

**[Your Name]**
- LinkedIn: [your-profile]
- Portfolio: [your-website]
- Email: [your-email]

---

## 🙏 Acknowledgments

- Data Source: [Bureau of Transportation Statistics](https://www.transtats.bts.gov/)
- Inspired by real-world airline operations analytics

---

**⭐ If you found this project helpful, please consider giving it a star!**

https://public.tableau.com/authoring/AirlineYoYChange/Sheet1#1
