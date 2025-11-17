# Weather ETL Pipeline (Python + API)

This project demonstrates a simple but realistic **ETL (Extract–Transform–Load)** workflow using Python, a public weather API, and basic data-cleaning techniques. It’s designed to showcase practical skills relevant to technical support engineering, solutions engineering, and data-integration roles.

The script fetches hourly weather data from the Open-Meteo API, transforms the JSON response into a clean tabular format, and saves both the raw and processed outputs for analysis.

---

## 🔍 Project Overview

**Extract:**  
- Calls the Open-Meteo public API (no API key required)  
- Retrieves hourly weather data (temperature & humidity) for Toronto, ON  

**Transform:**  
- Normalizes JSON into a pandas DataFrame  
- Converts timestamps  
- Drops invalid or empty rows  

**Load:**  
- Saves a timestamped raw JSON snapshot → `data/raw/`  
- Saves a cleaned CSV output → `data/processed/`

This ETL pattern is commonly used in:
- API integrations  
- Data onboarding  
- Troubleshooting customer data pipelines  
- Solutions Engineering workflows  
- Data automation and reporting  

---

## 🗂 Repository Structure

```
weather-etl-pipeline/
│
├── README.md
├── requirements.txt
│
├── src/
│   └── weather_etl.py
│
└── data/
    ├── raw/          # Raw API JSON (gitignored)
    └── processed/    # Clean CSV outputs (gitignored)
```

---

## ▶️ How to Run

### Install dependencies:
```bash
pip install -r requirements.txt
```

### Run the script:
```bash
python src/weather_etl.py
```

You should see console output showing extraction, transformation, and file-saving steps.

Output files will appear in:
- `data/raw/`
- `data/processed/`

---

## 🧪 Example Output (CSV)

Columns include:

| time                | temperature_2m | relativehumidity_2m |
|--------------------|----------------|----------------------|
| 2025-05-12 09:00   | 14.2           | 63                   |
| 2025-05-12 10:00   | 15.1           | 58                   |

(Timestamp and values will vary.)

---

## 🔧 Configuration

Modify these constants in `weather_etl.py`:

```python
LATITUDE = 43.65
LONGITUDE = -79.38
HOURLY_VARS = ["temperature_2m", "relativehumidity_2m"]
```

You can fetch data for any location by adjusting these values.

---

## 🚀 Future Enhancements

- Add CLI arguments (`--city`, `--lat`, `--lon`)
- Add error handling, logging, and retries
- Add unit tests for transform function  
- Add Dockerfile  
- Load data into a database instead of CSV  
- Build a small dashboard visualizing results  

---

## 📌 Purpose

This project was built to demonstrate:

- Working with REST APIs  
- ETL fundamentals  
- JSON → DataFrame transformation  
- Python scripting  
- Organizing a small technical project  
- Producing artifacts similar to real-world customer integration work  

It serves as a practical, portfolio-ready example of hands-on technical skill for roles in Solutions Engineering, Technical Support Engineering, and Customer Success Engineering.

---

Feel free to explore, fork, or adapt!
