import pyodbc
import pandas as pd
import os
import re
import sys
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from config import Config

# Load configuration
Config.load()

# Database connection configuration using secure credentials
DRIVER = '{ODBC Driver 17 for SQL Server}'
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROCESSED_LOG = os.path.join(SCRIPT_DIR, 'processed_csv_files.csv')

# Define measurement mapping
MEASUREMENT_MAPPING = {
    'PRED_GLOB_ctrl': 'global radiation',
    'PRED_RELHUM_2M_ctrl': 'relative humidity at 2 meters above ground',
    'PRED_TOT_PREC_ctrl': 'rain',
    'PRED_T_2M_ctrl': 'temperature at 2 meters above ground'
}

def connect_to_db():
    """Establish connection to the SQL Server database."""
    # Use credentials from Windows Credential Manager via Config
    server = Config.SERVER.replace('\x00', '')
    database = Config.DATABASE.replace('\x00', '')

    try:
        conn_string = (
            f'DRIVER={DRIVER};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'Trusted_Connection=yes'
        )
        
        conn = pyodbc.connect(conn_string)
        print("✅ Successfully connected to SQL Server!")
        return conn
    except Exception as e:
        print(f"❌ Error connecting to SQL Server: {e}")
        raise

def extract_file_date(filename: str) -> Optional[datetime]:
    """Extract the date from the filename."""
    # Update regex to match format Pred_YYYY-MM-DD.csv
    pattern = r'Pred_(\d{4}-\d{2}-\d{2})\.csv$'
    match = re.search(pattern, filename)
    
    if match:
        date_str = match.group(1)
        try:
            # Parse YYYY-MM-DD format
            return datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            print(f"❌ Invalid date format in filename: {filename}")
    else:
        print(f"No date match found in filename: {filename}")
    return None

def get_processed_files():
    """Get list of already processed files."""
    if not os.path.exists(PROCESSED_LOG):
        # Create empty DataFrame with headers
        df = pd.DataFrame(columns=['filename', 'file_date', 'process_date', 'forecast_start', 'forecast_end'])
        df.to_csv(PROCESSED_LOG, index=False)
        print(f"Created new processing log at: {PROCESSED_LOG}")
        return df
    
    # Read the existing log
    return pd.read_csv(PROCESSED_LOG)

def mark_file_as_processed(filename, file_date, forecast_start, forecast_end):
    """Add file to the processed files log."""
    processed_df = get_processed_files()
    
    # Add new row
    new_row = {
        'filename': filename,
        'file_date': file_date.strftime('%Y-%m-%d'),
        'process_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'forecast_start': forecast_start,
        'forecast_end': forecast_end
    }
    
    processed_df = pd.concat([processed_df, pd.DataFrame([new_row])], ignore_index=True)
    processed_df.to_csv(PROCESSED_LOG, index=False)

def load_csv_file(file_path: str) -> Tuple[pd.DataFrame, Dict, datetime]:
    """Load data from a CSV file and process it for weather prediction."""
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"❌ CSV file not found at {file_path}")
        
        # Extract the forecast source date from the filename
        filename = os.path.basename(file_path)
        file_date = extract_file_date(filename)
        
        if not file_date:
            raise ValueError(f"❌ Could not extract date from filename: {filename}")
            
        df = pd.read_csv(file_path)
        print(f"CSV columns: {df.columns.tolist()}")
        
        # Process the CSV data based on actual format
        # Convert Time to datetime format
        df['Date'] = pd.to_datetime(df['Time'], format='%d-%m-%Y')
        
        # Extract Date & Time Components
        df['Year'] = df['Date'].dt.year
        df['Month'] = df['Date'].dt.month
        df['Day'] = df['Date'].dt.day
        
        # Process Hour column to extract hour and minute
        df['TimeObj'] = pd.to_datetime(df['Hour'], format='%H:%M:%S%z')
        df['HourValue'] = df['TimeObj'].dt.hour
        df['MinuteValue'] = df['TimeObj'].dt.minute
        
        # Map measurement values using the predefined mapping
        df['MappedMeasurement'] = df['Measurement'].map(MEASUREMENT_MAPPING)
        
        # If no mapping exists, keep original value
        df['MappedMeasurement'] = df['MappedMeasurement'].fillna(df['Measurement'])
        
        # Print before and after examples to confirm mapping is working
        sample_measurements = df[['Measurement', 'MappedMeasurement']].drop_duplicates().head(10)
        print("Sample measurement mappings:")
        for _, row in sample_measurements.iterrows():
            print(f"  {row['Measurement']} -> {row['MappedMeasurement']}")
        
        print(f"Loaded and processed CSV file with {len(df)} rows")
        return df, MEASUREMENT_MAPPING, file_date
    except Exception as e:
        print(f"❌ Error loading CSV file {file_path}: {e}")
        raise

def load_dates(conn, df: pd.DataFrame):
    """Load unique dates into DimDate table."""
    cursor = conn.cursor()
    print("Loading dates...")
    
    inserted_count = 0
    unique_dates = df[['Year', 'Month', 'Day']].drop_duplicates().values
    
    for year, month, day in unique_dates:
        # Convert NumPy types to Python native types
        year_int = int(year)
        month_int = int(month)
        day_int = int(day)
        
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM DimDate WHERE year=? AND month=? AND day=?)
            INSERT INTO DimDate (year, month, day) VALUES (?, ?, ?)
        """, year_int, month_int, day_int, year_int, month_int, day_int)
        inserted_count += 1
    
    conn.commit()
    print(f"Processed {inserted_count} unique dates in DimDate")

def load_times(conn, df: pd.DataFrame):
    """Load unique times into DimTime table."""
    cursor = conn.cursor()
    print("Loading times...")
    
    inserted_count = 0
    unique_times = df[['HourValue', 'MinuteValue']].drop_duplicates().values
    
    for hour, minute in unique_times:
        # Convert NumPy types to Python native types
        hour_int = int(hour)
        minute_int = int(minute)
        
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM DimTime WHERE hour=? AND minute=?)
            INSERT INTO DimTime (hour, minute) VALUES (?, ?)
        """, hour_int, minute_int, hour_int, minute_int)
        inserted_count += 1
    
    conn.commit()
    print(f"Processed {inserted_count} unique times in DimTime")

def load_locations(conn, df: pd.DataFrame):
    """Load unique locations into DimLocation table."""
    cursor = conn.cursor()
    print("Loading locations...")
    
    inserted_count = 0
    unique_sites = df['Site'].unique()
    
    for site in unique_sites:
        # Ensure site is a Python string
        site_str = str(site)
        
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM DimLocation WHERE siteName=?)
            INSERT INTO DimLocation (siteName) VALUES (?)
        """, site_str, site_str)
        inserted_count += 1
    
    conn.commit()
    print(f"Processed {inserted_count} unique locations in DimLocation")

def load_measurements(conn, df: pd.DataFrame):
    """Load measurement types into DimMeasurement table."""
    cursor = conn.cursor()
    print("Loading measurements...")
    
    inserted_count = 0
    
    # Get unique measurement values from the MappedMeasurement column
    unique_measurements = df[['MappedMeasurement', 'Measurement']].drop_duplicates().values
    
    for mapped_measurement, original_measurement in unique_measurements:
        # Ensure values are Python strings
        mapped_measurement_str = str(mapped_measurement)
        
        # Get the corresponding unit for this measurement (first occurrence)
        unit = df[df['Measurement'] == original_measurement]['Unit'].iloc[0]
        unit_str = str(unit)
        
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM DimMeasurement WHERE measurement=?)
            INSERT INTO DimMeasurement (measurement, unit) VALUES (?, ?)
        """, mapped_measurement_str, mapped_measurement_str, unit_str)
        inserted_count += 1
    
    conn.commit()
    print(f"Processed {inserted_count} measurement types in DimMeasurement")

def get_date_ids_for_range(conn, start_date, end_date):
    """Get all date IDs for a date range."""
    cursor = conn.cursor()
    date_ids = []
    
    try:
        cursor.execute("""
            SELECT d.idDate 
            FROM DimDate d
            WHERE DATEFROMPARTS(d.year, d.month, d.day) BETWEEN ? AND ?
        """, start_date, end_date)
        
        date_ids = [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"❌ Error getting date IDs: {e}")
    
    return date_ids

def delete_forecasts_for_dates(conn, date_ids):
    """Delete forecasts for the given date IDs."""
    if not date_ids:
        print("No dates to delete forecasts for")
        return 0
        
    cursor = conn.cursor()
    deleted_count = 0
    
    try:
        # Process in chunks to avoid too large IN clause
        chunk_size = 500
        for i in range(0, len(date_ids), chunk_size):
            chunk = date_ids[i:i+chunk_size]
            id_list = ','.join(map(str, chunk))
            
            cursor.execute(f"""
                DELETE FROM Fact_WeatherPrediction
                WHERE idDate IN ({id_list})
            """)
            
            deleted_count += cursor.rowcount
            conn.commit()
            
        print(f"Deleted {deleted_count} existing forecast records")
    except Exception as e:
        print(f"❌ Error deleting forecasts: {e}")
        conn.rollback()
        
    return deleted_count

def load_weather_facts(conn, df: pd.DataFrame):
    """Load weather prediction data into Fact_WeatherPrediction table."""
    cursor = conn.cursor()
    print("Loading weather prediction facts...")
    
    inserted_count = 0
    updated_count = 0
    batch_size = 1000  # Process in batches to avoid memory issues
    total_rows = len(df)
    
    # Process the dataframe in batches
    for start_idx in range(0, total_rows, batch_size):
        end_idx = min(start_idx + batch_size, total_rows)
        batch_df = df.iloc[start_idx:end_idx]
        
        for _, row in batch_df.iterrows():
            try:
                # Retrieve Foreign Keys - convert NumPy types to Python native types
                cursor.execute("SELECT idDate FROM DimDate WHERE year=? AND month=? AND day=?", 
                              int(row['Year']), int(row['Month']), int(row['Day']))
                idDate_result = cursor.fetchone()
                if idDate_result is None:
                    print(f"Warning: No matching date found for {row['Year']}-{row['Month']}-{row['Day']}")
                    continue
                idDate = idDate_result[0]

                cursor.execute("SELECT idTime FROM DimTime WHERE hour=? AND minute=?", 
                              int(row['HourValue']), int(row['MinuteValue']))
                idTime_result = cursor.fetchone()
                if idTime_result is None:
                    print(f"Warning: No matching time found for {row['HourValue']}:{row['MinuteValue']}")
                    continue
                idTime = idTime_result[0]
              
                cursor.execute("SELECT idLocation FROM DimLocation WHERE siteName=?", 
                              str(row['Site']))
                idLocation_result = cursor.fetchone()
                if idLocation_result is None:
                    print(f"Warning: No matching location found for {row['Site']}")
                    continue
                idLocation = idLocation_result[0]

                # Use the mapped measurement name
                mapped_measurement_str = str(row['MappedMeasurement'])
                cursor.execute("SELECT idMeasurement FROM DimMeasurement WHERE measurement=?", 
                             mapped_measurement_str)
                idMeasurement_result = cursor.fetchone()
                if idMeasurement_result is None:
                    print(f"Warning: No matching measurement found for {mapped_measurement_str}")
                    continue
                idMeasurement = idMeasurement_result[0]
                
                # Use 'Value' column for the value
                valueMeasurement = float(row['Value'])

                # Check if the record already exists
                cursor.execute("""
                    SELECT valueMeasurement FROM Fact_WeatherPrediction
                    WHERE idDate=? AND idTime=? AND idMeasurement=? AND idLocation=?
                """, idDate, idTime, idMeasurement, idLocation)
                
                existing = cursor.fetchone()
                
                if existing:
                    # Update existing record
                    cursor.execute("""
                        UPDATE Fact_WeatherPrediction
                        SET valueMeasurement = ?
                        WHERE idDate=? AND idTime=? AND idMeasurement=? AND idLocation=?
                    """, valueMeasurement, idDate, idTime, idMeasurement, idLocation)
                    updated_count += 1
                else:
                    # Insert new record
                    cursor.execute("""
                        INSERT INTO Fact_WeatherPrediction 
                        (idDate, idTime, idMeasurement, idLocation, valueMeasurement)
                        VALUES (?, ?, ?, ?, ?)
                    """, idDate, idTime, idMeasurement, idLocation, valueMeasurement)
                    inserted_count += 1
                
                # Commit every 1000 operations to avoid transaction log overflow
                if (inserted_count + updated_count) % 1000 == 0:
                    conn.commit()
                    print(f"Committed {inserted_count} inserts and {updated_count} updates so far...")
                    
            except Exception as e:
                print(f"❌ Error processing row: {e}")
                print(f"Row data: {row}")
                continue
    
    # Final commit
    conn.commit()
    print(f"Inserted {inserted_count} and updated {updated_count} records in Fact_WeatherPrediction")

def should_process_file(filename, file_date, processed_files_df):
    """Determine if a file should be processed based on its date and processing history."""
    if not processed_files_df.empty:
        # Check if the file has already been processed
        if filename in processed_files_df['filename'].values:
            print(f"File {filename} has already been processed. Skipping.")
            return False
        
        # Check if there are any newer files that have already been processed
        processed_dates = pd.to_datetime(processed_files_df['file_date'])
        newer_files = processed_files_df[processed_dates > file_date]
        
        if not newer_files.empty:
            print(f"Skipping {filename} as newer files have already been processed:")
            for _, row in newer_files.iterrows():
                print(f"  - {row['filename']} (date: {row['file_date']})")
            return False
    
    return True

def process_csv_files_in_directory(directory: str):
    """Process all CSV files in the given directory."""
    try:
        # Connect to the database
        print("Connecting to database...")
        conn = connect_to_db()
        
        # Get list of already processed files
        processed_files_df = get_processed_files()
        print(f"Found {len(processed_files_df)} processed files in log: {PROCESSED_LOG}")
        
        # Get list of all CSV files in the directory
        processed_log_filename = os.path.basename(PROCESSED_LOG)
        all_csv_files = [f for f in os.listdir(directory) 
                        if f.endswith('.csv') and f != processed_log_filename]
        print(f"Found {len(all_csv_files)} CSV files in {directory}")
        
        # Prepare files with dates
        file_with_dates = []
        for filename in all_csv_files:
            file_date = extract_file_date(filename)
            if file_date:
                file_with_dates.append((filename, file_date))
            else:
                print(f"Could not extract date from {filename}, skipping")
        
        if not file_with_dates:
            print("No valid files with dates found to process. Exiting.")
            return
        
        # Sort by date (newest first)
        sorted_files = sorted(file_with_dates, key=lambda x: x[1], reverse=True)
        print(f"Sorted {len(sorted_files)} files by date (newest first)")
        
        # Process files in order (newest first)
        for i, (filename, file_date) in enumerate(sorted_files):
            file_path = os.path.join(directory, filename)
            print(f"Processing file {i+1}/{len(sorted_files)}: {file_path} (Date: {file_date})")
            
            # Check if we should process this file
            if not should_process_file(filename, file_date, processed_files_df):
                continue
            
            # Load and process the CSV file
            df, _, _ = load_csv_file(file_path)
            
            # Get forecast date range
            forecast_start_date = df['Date'].min().strftime('%Y-%m-%d')
            forecast_end_date = df['Date'].max().strftime('%Y-%m-%d')
            
            print(f"Forecast range in this file: {forecast_start_date} to {forecast_end_date}")
            
            # Load dimension data (do this first to ensure all needed dimensions exist)
            load_dates(conn, df)
            load_times(conn, df)
            load_locations(conn, df)
            load_measurements(conn, df)
            
            # Get date IDs for the forecast range and delete existing forecasts
            date_ids = get_date_ids_for_range(conn, forecast_start_date, forecast_end_date)
            delete_forecasts_for_dates(conn, date_ids)
            
            # Load the new forecast data
            load_weather_facts(conn, df)
            
            # Mark file as processed
            mark_file_as_processed(filename, file_date, forecast_start_date, forecast_end_date)
            
            # Print progress
            print(f"Completed file {i+1}/{len(sorted_files)}: {filename}")
        
        # Close connection
        conn.close()
        print("✅ All data successfully inserted into SQL Server!")
        
    except Exception as e:
        print(f"❌ Error processing CSV files: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main function to load all data from WEATHER_DATA_DIR."""
    try:
        print(f"Script is running from: {SCRIPT_DIR}")
        print(f"Processing log will be stored at: {PROCESSED_LOG}")
        print(f"Data directory: {Config.WEATHER_DATA_DIR}")
        
        # Use the WEATHER_DATA_DIR from Config
        process_csv_files_in_directory(Config.WEATHER_DATA_DIR)
        
    except Exception as e:
        print(f"❌ Error in main function: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()