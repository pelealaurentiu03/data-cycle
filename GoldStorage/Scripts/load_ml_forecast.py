import os
import pandas as pd
import pyodbc
import re
from datetime import datetime
from config import Config

# Load configuration
Config.load()

# Database connection configuration using secure credentials
DRIVER = '{ODBC Driver 17 for SQL Server}'

# Define mapping constants
FORECAST_TYPE_MAPPING = {
    'motion': 'Room presence',
    'power_consumption': 'Power consumption'
}

BUILDING_MAPPING = {
    'Apartment_1': 'JeremieVianin',
    'Apartment_2': 'JimmyLoup'
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

def extract_info_from_filename(filename):
    """Extract apartment, room, and forecast type from filename."""
    # Define patterns to extract information
    apartment_pattern = r'(Apartment_\d+)'
    
    # Special case for power consumption files
    if '_power_consumption_forecast.csv' in filename:
        apartment_match = re.search(apartment_pattern, filename)
        apartment = apartment_match.group(1) if apartment_match else None
        room = 'House'
        forecast_type = 'power_consumption'
    else:
        # Pattern for motion files
        room_pattern = r'(?:Apartment_\d+)_([A-Za-z]+)_([A-Za-z]+)_forecast\.csv'
        
        # Extract apartment
        apartment_match = re.search(apartment_pattern, filename)
        apartment = apartment_match.group(1) if apartment_match else None
        
        # Extract room and forecast type
        room_forecast_match = re.search(room_pattern, filename)
        
        if room_forecast_match:
            room = room_forecast_match.group(1)
            forecast_type = room_forecast_match.group(2)
        else:
            room = None
            forecast_type = None
    
    return apartment, room, forecast_type

def get_or_create_date_id(conn, date_str):
    """Get existing date ID or create a new one."""
    cursor = conn.cursor()
    
    try:
        # Parse the date string
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        year = date_obj.year
        month = date_obj.month
        day = date_obj.day
        
        # Check if date exists
        cursor.execute("""
            SELECT idDate FROM DimDate WHERE year=? AND month=? AND day=?
        """, year, month, day)
        
        result = cursor.fetchone()
        if result:
            return result[0]
        
        # Create new date
        cursor.execute("""
            INSERT INTO DimDate (year, month, day) VALUES (?, ?, ?)
        """, year, month, day)
        
        # Get the ID of the newly inserted record
        cursor.execute("SELECT @@IDENTITY")
        new_id = cursor.fetchone()[0]
        
        conn.commit()
        return new_id
    except Exception as e:
        print(f"❌ Error in get_or_create_date_id: {e}")
        conn.rollback()
        raise

def get_or_create_time_id(conn, time_str):
    """Get existing time ID or create a new one."""
    cursor = conn.cursor()
    
    try:
        # Parse the time string
        time_parts = time_str.split(':')
        hour = int(time_parts[0])
        minute = int(time_parts[1]) if len(time_parts) > 1 else 0
        
        # Check if time exists
        cursor.execute("""
            SELECT idTime FROM DimTime WHERE hour=? AND minute=?
        """, hour, minute)
        
        result = cursor.fetchone()
        if result:
            return result[0]
        
        # Create new time
        cursor.execute("""
            INSERT INTO DimTime (hour, minute) VALUES (?, ?)
        """, hour, minute)
        
        # Get the ID of the newly inserted record
        cursor.execute("SELECT @@IDENTITY")
        new_id = cursor.fetchone()[0]
        
        conn.commit()
        return new_id
    except Exception as e:
        print(f"❌ Error in get_or_create_time_id: {e}")
        conn.rollback()
        raise

def get_room_id(conn, room_name):
    """Get room ID from DimRoom."""
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT idRoom FROM DimRoom WHERE roomName=?
        """, room_name)
        
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            print(f"Warning: Room '{room_name}' not found in DimRoom. Adding it.")
            cursor.execute("""
                INSERT INTO DimRoom (roomName) VALUES (?)
            """, room_name)
            
            cursor.execute("SELECT @@IDENTITY")
            new_id = cursor.fetchone()[0]
            conn.commit()
            return new_id
    except Exception as e:
        print(f"❌ Error in get_room_id: {e}")
        raise

def get_building_id(conn, house_name):
    """Get building ID from DimBuilding."""
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT idBuilding FROM DimBuilding WHERE houseName=?
        """, house_name)
        
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            print(f"Warning: Building with houseName '{house_name}' not found in DimBuilding.")
            return None
    except Exception as e:
        print(f"❌ Error in get_building_id: {e}")
        raise

def process_csv_file(conn, file_path):
    """Process a single CSV file and load data into Fact_MachineLearning."""
    print(f"Processing file: {file_path}")
    
    try:
        # Extract filename from path
        filename = os.path.basename(file_path)
        
        # Extract information from filename
        apartment, room, forecast_type = extract_info_from_filename(filename)
        
        if not all([apartment, room, forecast_type]):
            print(f"❌ Could not extract all required information from filename: {filename}")
            return 0
        
        # Map forecast type to display value
        forecast_type_display = FORECAST_TYPE_MAPPING.get(forecast_type, forecast_type)
        
        # Get building name from mapping
        house_name = BUILDING_MAPPING.get(apartment)
        
        if not house_name:
            print(f"❌ Could not map apartment '{apartment}' to a building name")
            return 0
        
        # Get building ID
        building_id = get_building_id(conn, house_name)
        
        if not building_id:
            print(f"❌ Could not find building ID for '{house_name}'")
            return 0
        
        # Get room ID
        room_id = get_room_id(conn, room)
        
        # Read the CSV file
        df = pd.read_csv(file_path)
        
        # Determine which value column to use
        value_column = 'Predicted_Presence' if 'Predicted_Presence' in df.columns else 'TotalPower_Sum'
        
        # Process each row in the CSV
        rows_processed = 0
        cursor = conn.cursor()
        
        for _, row in df.iterrows():
            try:
                # Get date ID
                date_id = get_or_create_date_id(conn, row['DateTime'])
                
                # Get time ID
                time_id = get_or_create_time_id(conn, row['Hours'])
                
                # Get forecast date (already in correct format)
                forecast_date = row['Forecast_Date']
                
                # Get forecast value
                forecast_value = float(row[value_column])
                
                # Check if record already exists
                cursor.execute("""
                    SELECT 1 FROM Fact_MachineLearning 
                    WHERE idDate=? AND idTime=? AND idBuilding=? AND idRoom=? AND ForecastDate=?
                """, date_id, time_id, building_id, room_id, forecast_date)
                
                if cursor.fetchone():
                    # Update existing record
                    cursor.execute("""
                        UPDATE Fact_MachineLearning 
                        SET ForecastType=?, ForecastValue=?
                        WHERE idDate=? AND idTime=? AND idBuilding=? AND idRoom=? AND ForecastDate=?
                    """, forecast_type_display, forecast_value, date_id, time_id, building_id, room_id, forecast_date)
                else:
                    # Insert new record
                    cursor.execute("""
                        INSERT INTO Fact_MachineLearning 
                        (idDate, idTime, idBuilding, idRoom, ForecastDate, ForecastType, ForecastValue)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, date_id, time_id, building_id, room_id, forecast_date, forecast_type_display, forecast_value)
                
                rows_processed += 1
                
                # Commit every 50 rows to avoid large transactions
                if rows_processed % 50 == 0:
                    conn.commit()
                    print(f"Committed {rows_processed} rows from {filename}")
                
            except Exception as e:
                print(f"❌ Error processing row in file {filename}: {e}")
                print(f"Row data: {row}")
                conn.rollback()
                continue
        
        # Final commit for any remaining rows
        conn.commit()
        print(f"✅ Successfully processed {rows_processed} rows from {filename}")
        
        return rows_processed
    
    except Exception as e:
        print(f"❌ Error processing file {file_path}: {e}")
        import traceback
        traceback.print_exc()
        return 0

def process_all_forecast_files():
    """Process all machine learning forecast CSV files."""
    try:
        # Get directory path from Config
        if not hasattr(Config, 'ML_FORECASTS_DIR'):
            print("❌ ML_FORECASTS_DIR not found in configuration.")
            print("Please add ML_FORECASTS_DIR to your config.ini file.")
            return
        
        forecast_dir = Config.ML_FORECASTS_DIR
        print(f"Looking for forecast files in: {forecast_dir}")
        
        # Connect to the database
        conn = connect_to_db()
        
        # Get list of CSV files
        csv_files = [f for f in os.listdir(forecast_dir) if f.endswith('_forecast.csv')]
        print(f"Found {len(csv_files)} forecast CSV files")
        
        # Process each file
        total_rows_processed = 0
        for filename in csv_files:
            file_path = os.path.join(forecast_dir, filename)
            rows_processed = process_csv_file(conn, file_path)
            total_rows_processed += rows_processed
        
        # Close the database connection
        conn.close()
        
        print(f"✅ Total rows processed across all files: {total_rows_processed}")
    
    except Exception as e:
        print(f"❌ Error in process_all_forecast_files: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main function to load forecast data."""
    print(f"Starting ML forecast data loading process...")
    start_time = datetime.now()
    
    try:
        # Process all forecast files
        process_all_forecast_files()
        
        end_time = datetime.now()
        duration = end_time - start_time
        print(f"Completed ML forecast loading at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total execution time: {duration}")
    
    except Exception as e:
        print(f"❌ Error in main function: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()