# YouTube-Global-Trends
End-to-end data project that collects YouTube trending videos worldwide using the YouTube Data API v3, processes them with Python, and visualizes insights in Tableau.

## Project overview
This repository fetches trending videos from YouTube Top Charts across regions, cleans and enriches the data, and prepares a dataset for exploratory analysis and Tableau dashboards.

**Key goals**
- Retrieve trending videos for many regions using the YouTube Data API v3.
- Clean, standardize, and enrich the data (e.g., duration buckets, continent).
- Produce a processed CSV ready for analysis and Tableau visualizations.
- Answer example questions:
  - Which categories dominate YouTube globally?
  - How do video durations vary across regions?
  - Which regions show distinct viewing patterns?
  - Is there a pattern between view count and like count?

## Tech stack
- **YouTube Data API v3** — video metadata  
- **Python** (Pandas, NumPy) — data collection & preprocessing  
- **Tableau** — dashboarding & visualization

### Repository Structure
```
├── scripts/
│   ├── fetch_youtube_data.py   # API integration & data retrieval
│   ├── process_data.py         # Cleaning & transformations
├── notebooks/
│   ├── eda.ipynb               # Exploratory data analysis
├── data/
│   ├── raw/                    # API results
│   ├── processed/              # Clean dataset for Tableau
├── tableau/
│   ├── dashboard.twbx          # Tableau workbook
├── visuals/
│   ├── sample_dashboard.png    # Preview of Tableau insights
└── README.md
```

### Understand the Dataset
- `region` — ISO code or region name  
- `title` — video title  
- `channelTitle` — channel name  
- `publishedAt` — publication timestamp
- `viewCount` — View Count 
- `likeCount` — Like Count
- `commentCount` — Comment Count  
- `duration` — Duration (hh:mm:ss) 
- `description` — Video Description Text  
- `categoryName` — Category (e.g., Music, Gaming,...)  
- `continent` — Continent derived from Region
- `duration_category` — `short` / `medium` / `long` Duration Category: derived from duration 

### Tableau Dashboard Preview
Tableau
tableau/dashboard.twbx is a sample Tableau workbook built from data/processed/trending_clean.csv.

#### Future Improvements
- Automate daily API collection with cron jobs or Airflow.
- Add sentiment analysis of video comments.
- Predict video virality using machine learning.
