# Smart Apartment Data Pipeline & Analytics

I developed this end-to-end data engineering and analytics project during the Erasmus Blended Intensive Program in Switzerland. The main objective was to manage the complete data lifecycle of smart apartment sensors and weather data, taking it from raw extraction to predictive forecasting and business visualization.

Coming from an economics background and transitioning into Big Data, I wanted to ensure this technical project delivered clear business value. To achieve this, I structured the data flow using the industry-standard Medallion Architecture, ensuring high data quality before feeding it into machine learning models and reporting tools.

## Pipeline Architecture

* Bronze Layer (Extraction): I wrote automated Python scripts to fetch and store raw sensor and weather data, establishing the foundation of the data environment.
* Silver Layer (Transformation): Because raw data is rarely ready for analysis, I implemented cleaning scripts to handle anomalies, format inconsistencies, and missing values.
* Gold Layer (Loading): The validated data is aggregated and structured using SQL, making it optimized and ready for downstream analytics.

## Machine Learning & Visualization

* Forecasting Models: Using the structured historical data, I conducted exploratory data analysis and trained Random Forest regressors in Jupyter Notebooks to predict power consumption and room occupation trends.
* Business Intelligence: I designed a comprehensive Power BI dashboard connected to the Gold layer, providing interactive KPIs and actionable insights for stakeholders. Note: Due to GitHub file size constraints for the interactive .pbix file (28 MB), comprehensive screenshots and dashboard previews are included in the project documentation.

## Technical Stack

* ETL & Data Processing: Python (Pandas)
* Storage & Database: SQL
* Machine Learning: Scikit-Learn, Jupyter
* Visualization: Power BI

## Repository Structure

* /Extract - Python scripts for fetching raw sensor and weather data via APIs.
* /Clean - Data transformation, cleaning, and formatting scripts.
* /Load & /GoldStorage/Scripts - Scripts dedicated to loading the processed data into the database architecture.
* /Training - Jupyter notebooks detailing the exploratory data analysis and ML model training process.
* /Forecasts - Deployment scripts for running the trained predictive models.
* /Docs - Technical documentation, manuals, presentations, and visual previews of the Power BI dashboard.
* Apartment.sql - The complete database schema and table creation queries.

## Note on Running Locally

For security and privacy reasons, all database credentials, API keys, and sensitive configuration files (such as config.ini and setup_credentials.py) have been excluded from this public repository via .gitignore. To run these scripts locally, you must configure your own local environment variables.
