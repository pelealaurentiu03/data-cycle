# Smart Apartment Data Pipeline & Analytics

I developed this end-to-end data engineering and analytics project during the Erasmus Blended Intensive Program in Switzerland. The main objective was to manage the complete data lifecycle of smart apartment sensors and weather data, taking it from raw extraction to predictive forecasting and business visualization.

Coming from an economics background and transitioning into Big Data, I wanted to ensure this technical project delivered clear business value. To achieve this, I structured the data flow using the industry-standard Medallion Architecture, ensuring high data quality before feeding it into machine learning models and reporting tools.

## Pipeline Architecture

* Bronze Layer (Extraction): I wrote automated Python scripts to fetch and store raw sensor and weather data, establishing the foundation of the data environment.
* Silver Layer (Transformation): Because raw data is rarely ready for analysis, I implemented cleaning scripts to handle anomalies, format inconsistencies, and missing values.
* Gold Layer (Loading): The validated data is aggregated and structured using SQL, making it optimized and ready for downstream analytics.

## Machine Learning & Visualization

* Forecasting Models: Using the structured historical data, I conducted exploratory data analysis and trained Random Forest regressors in Jupyter Notebooks to predict power consumption and room occupation trends.
* Business Intelligence: I designed a comprehensive Power BI dashboard connected to the Gold layer, providing interactive KPIs and actionable insights for stakeholders.

## Technical Stack

* ETL & Data Processing: Python (Pandas)
* Storage & Database: SQL
* Machine Learning: Scikit-Learn, Jupyter
* Visualization: Power BI

## Repository Structure

* /src - Contains the Python modules separated by pipeline stage (Extract, Clean, Load, Forecast).
* /notebooks - Jupyter notebooks detailing the model training and evaluation process.
* /sql - Database schemas and relevant queries.
* /dashboard - The final Power BI report file.
* /docs - Technical documentation, manuals, and project presentations.

## Note on Running Locally

For security and privacy reasons, all database credentials, API keys, and sensitive configuration files (such as `config.ini` and `setup_credentials.py`) have been excluded from this public repository. To run these scripts locally, you must configure your own local environment variables.
