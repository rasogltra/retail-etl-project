## Retail Sales ETL Pipeline
This project implements Extract, Transform and Load (ETL) pipline to process retail sales data, clean and load into database for analytics.

### Features
- **Data Extraction**: Reads retail sales data from CSV files.
- **Data Transformation**: Handles missing values, standardizes formats and validates data types. 
- **Data Loading**: Inserts cleaned data into a SQlite database.
- **Reporting**: Generates summary reports on retail sales.

### Technologies Used
- **Python 3.9+**
- **Pandas**
- **SQLAlchemy**

### Setup and Run
1. Place your CSV files in `data/raw`.
2. Configure paths in `config/settings.ini`
3. Run the ETL

```bash
cd src
python etl_pipeline.py
````