import json
import pyodbc
import os
from typing import List, Dict, Any
from datetime import datetime
import glob
import sys
from config import Config
import concurrent.futures
import threading
from queue import Queue

# Load configuration
Config.load()

# Database connection configuration using secure credentials
DRIVER = '{ODBC Driver 17 for SQL Server}'
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) # Get the directory of the executing script
PROCESSED_LOG = os.path.join(SCRIPT_DIR, 'processed_json_files.csv') # File to track processed files

thread_local = threading.local()
processed_files_lock = threading.Lock()
processed_files_queue = Queue()  # Queue to collect processed files

# Room name corrections
ROOM_CORRECTIONS = {
    'Bdroom': 'Bedroom',
    'Bhroom': 'Bathroom'
}

# Define sensor type mapping to use existing types
SENSOR_TYPE_MAPPING = {
    'Door': 'Door/Window',
    'Window': 'Door/Window'
}

def get_thread_connection():
    """Get a database connection for the current thread."""
    if not hasattr(thread_local, "connection"):
        thread_local.connection = connect_to_db()
    return thread_local.connection

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

def get_processed_files():
    """Get list of already processed files."""
    import pandas as pd
    
    if not os.path.exists(PROCESSED_LOG):
        # Create empty DataFrame with headers
        df = pd.DataFrame(columns=['filepath', 'process_date'])
        df.to_csv(PROCESSED_LOG, index=False)
        print(f"Created new processing log at: {PROCESSED_LOG}")
        return df
    
    # Read the existing log
    return pd.read_csv(PROCESSED_LOG)

def mark_file_as_processed(filepath):
    """Add file to the processed files queue (not directly to CSV)."""
    processed_files_queue.put({
        'filepath': filepath,
        'process_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })

def should_process_file(filepath, processed_files_df):
    """Determine if a file should be processed."""
    if not processed_files_df.empty and filepath in processed_files_df['filepath'].values:
        print(f"File {os.path.basename(filepath)} has already been processed. Skipping.")
        return False
    return True

def load_json_file(file_path: str) -> Dict:
    """Load data from a JSON file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except Exception as e:
        print(f"❌ Error loading JSON file {file_path}: {e}")
        raise

def parse_datetime(date_str: str, time_str: str = "00:00") -> Dict:
    """Parse date and time string to get year, month, day, hour, minute."""
    try:
        # Try different date formats
        for fmt in ['%d/%m/%Y', '%m/%d/%Y', '%Y-%m-%d', '%d-%m-%Y', '%m-%d-%Y']:
            try:
                date_obj = datetime.strptime(date_str, fmt)
                break
            except ValueError:
                continue
        else:
            raise ValueError(f"Could not parse date: {date_str}")
        
        # Parse time
        if ":" in time_str:
            hour, minute = map(int, time_str.split(':'))
        else:
            hour, minute = 0, 0
            
        return {
            'year': date_obj.year,
            'month': date_obj.month,
            'day': date_obj.day,
            'hour': hour,
            'minute': minute
        }
    except Exception as e:
        print(f"❌ Error parsing datetime {date_str} {time_str}: {e}")
        raise

def correct_room_name(room_name: str) -> str:
    """Apply corrections to room names if needed."""
    return ROOM_CORRECTIONS.get(room_name, room_name)

def get_or_create_date_id(conn, year, month, day):
    """Get existing date ID or create a new one."""
    cursor = conn.cursor()
    
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

def get_or_create_time_id(conn, hour, minute):
    """Get existing time ID or create a new one."""
    cursor = conn.cursor()
    
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

def get_or_create_building_id(conn, building_data):
    """Get existing building ID or create a new one."""
    cursor = conn.cursor()
    
    # For our JSON examples, we'll use the User field as the building name
    building_name = building_data.get('User', 'Unknown')
    
    # Check if building exists
    cursor.execute("""
        SELECT idBuilding FROM DimBuilding WHERE houseName=?
    """, building_name)
    
    result = cursor.fetchone()
    if result:
        return result[0]
    
    # Create new building with minimal data
    cursor.execute("""
        INSERT INTO DimBuilding (buildingType, houseName, latitude, longitude, adress, npa, city, nbPeople, isHeatingOn)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, 'House', building_name, 0.0, 0.0, '', '', '', 0, 0)
    
    # Get the ID of the newly inserted record
    cursor.execute("SELECT @@IDENTITY")
    new_id = cursor.fetchone()[0]
    
    conn.commit()
    return new_id

def get_or_create_room_id(conn, room_name):
    """Get existing room ID or create a new one."""
    cursor = conn.cursor()
    
    # Apply room name corrections
    corrected_name = correct_room_name(room_name)
    
    # Check if room exists
    cursor.execute("""
        SELECT idRoom FROM DimRoom WHERE roomName=?
    """, corrected_name)
    
    result = cursor.fetchone()
    if result:
        return result[0]
    
    # Create new room
    cursor.execute("""
        INSERT INTO DimRoom (roomName) VALUES (?)
    """, corrected_name)
    
    # Get the ID of the newly inserted record
    cursor.execute("SELECT @@IDENTITY")
    new_id = cursor.fetchone()[0]
    
    conn.commit()
    return new_id

def get_or_create_sensor_id(conn, sensor_type):
    """Get existing sensor ID or create a new one."""
    cursor = conn.cursor()
    
    # Apply sensor type mapping if needed
    mapped_sensor_type = SENSOR_TYPE_MAPPING.get(sensor_type, sensor_type)
    
    # Check if sensor exists
    cursor.execute("""
        SELECT idSensor FROM DimSensor WHERE sensorType=?
    """, mapped_sensor_type)
    
    result = cursor.fetchone()
    if result:
        return result[0]
    
    # Create new sensor
    print(f"Creating new sensor type: {mapped_sensor_type}")
    cursor.execute("""
        INSERT INTO DimSensor (sensorType) VALUES (?)
    """, mapped_sensor_type)
    
    # Get the ID of the newly inserted record
    cursor.execute("SELECT @@IDENTITY")
    new_id = cursor.fetchone()[0]
    
    conn.commit()
    return new_id

def process_plugs(conn, data, date_id, time_id, building_id):
    """Process and insert plugs data."""
    cursor = conn.cursor()
    plugs_data = data.get('Plugs', {})
    sensor_id = get_or_create_sensor_id(conn, 'Plug')
    
    inserted_count = 0
    
    for room_name, plug_data in plugs_data.items():
        room_id = get_or_create_room_id(conn, room_name)
        
        try:
            # Check if record exists
            cursor.execute("""
                SELECT 1 FROM Fact_Plugs 
                WHERE idBuilding=? AND idRoom=? AND idSensor=? AND idDate=? AND idTime=?
            """, building_id, room_id, sensor_id, date_id, time_id)
            
            if cursor.fetchone():
                # Update existing record
                cursor.execute("""
                    UPDATE Fact_Plugs 
                    SET switch=?, temperature=?, overTemperature=?, counter1=?, counter2=?, counter3=?, 
                        power=?, overPower=?, timeplug=?, total=?
                    WHERE idBuilding=? AND idRoom=? AND idSensor=? AND idDate=? AND idTime=?
                """, 
                    1 if plug_data.get('Switch', False) else 0,
                    plug_data.get('Temperature', 0.0),
                    1 if plug_data.get('Overtemperature', False) else 0,
                    plug_data.get('Counter1', 0.0),
                    plug_data.get('Counter2', 0.0),
                    plug_data.get('Counter3', 0.0),
                    plug_data.get('Power', 0.0),
                    plug_data.get('Overpower', 0.0),
                    plug_data.get('Timeplug', 0),
                    plug_data.get('Total', 0),
                    building_id, room_id, sensor_id, date_id, time_id
                )
            else:
                # Insert new record
                cursor.execute("""
                    INSERT INTO Fact_Plugs (
                        idBuilding, idRoom, idSensor, idDate, idTime,
                        switch, temperature, overTemperature, counter1, counter2, counter3,
                        power, overPower, timeplug, total
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, 
                    building_id, room_id, sensor_id, date_id, time_id,
                    1 if plug_data.get('Switch', False) else 0,
                    plug_data.get('Temperature', 0.0),
                    1 if plug_data.get('Overtemperature', False) else 0,
                    plug_data.get('Counter1', 0.0),
                    plug_data.get('Counter2', 0.0),
                    plug_data.get('Counter3', 0.0),
                    plug_data.get('Power', 0.0),
                    plug_data.get('Overpower', 0.0),
                    plug_data.get('Timeplug', 0),
                    plug_data.get('Total', 0)
                )
                inserted_count += 1
                
        except Exception as e:
            print(f"❌ Error processing plug data for room {room_name}: {e}")
            continue
    
    conn.commit()
    return inserted_count

def process_doors_windows(conn, data, date_id, time_id, building_id):
    """Process and insert doors and windows data."""
    cursor = conn.cursor()
    doorswindows_data = data.get('Doorswindows', {})
    door_window_sensor_id = get_or_create_sensor_id(conn, 'Door/Window')
    
    inserted_count = 0
    
    for room_name, devices in doorswindows_data.items():
        room_id = get_or_create_room_id(conn, room_name)
        
        # First, delete all existing entries for this room/building/date/time
        try:
            cursor.execute("""
                DELETE FROM Fact_DoorsWindows 
                WHERE idBuilding=? AND idRoom=? AND idSensor=? AND idDate=? AND idTime=?
            """, building_id, room_id, door_window_sensor_id, date_id, time_id)
            conn.commit()
        except Exception as e:
            print(f"❌ Error deleting existing doors/windows data for room {room_name}: {e}")
            conn.rollback()
        
        # Then insert all devices for this room
        for device in devices:
            device_type = device.get('Type', 'Unknown')
            
            try:
                cursor.execute("""
                    INSERT INTO Fact_DoorsWindows (
                        idBuilding, idRoom, idSensor, idDate, idTime,
                        doorsWindowsType, battery, defense, switch
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, 
                    building_id, room_id, door_window_sensor_id, date_id, time_id,
                    device_type,
                    device.get('Battery', 0),
                    1 if device.get('Defense', 0) == 1 else 0,
                    1 if device.get('Switch', False) else 0
                )
                conn.commit()
                inserted_count += 1
                    
            except Exception as e:
                print(f"❌ Error inserting {device_type} in room {room_name}: {e}")
                conn.rollback()
                continue
    
    return inserted_count

def process_motions(conn, data, date_id, time_id, building_id):
    """Process and insert motion data."""
    cursor = conn.cursor()
    motions_data = data.get('Motions', {})
    sensor_id = get_or_create_sensor_id(conn, 'Motion')
    
    inserted_count = 0
    
    for room_name, motion_data in motions_data.items():
        room_id = get_or_create_room_id(conn, room_name)
        
        try:
            # Check if record exists
            cursor.execute("""
                SELECT 1 FROM Fact_Motions 
                WHERE idBuilding=? AND idRoom=? AND idSensor=? AND idDate=? AND idTime=?
            """, building_id, room_id, sensor_id, date_id, time_id)
            
            if cursor.fetchone():
                # Update existing record
                cursor.execute("""
                    UPDATE Fact_Motions 
                    SET motion=?, light=?, temperature=?
                    WHERE idBuilding=? AND idRoom=? AND idSensor=? AND idDate=? AND idTime=?
                """, 
                    1 if motion_data.get('Motion', False) else 0,
                    motion_data.get('Light', 0),
                    motion_data.get('Temperature', 0.0),
                    building_id, room_id, sensor_id, date_id, time_id
                )
            else:
                # Insert new record
                cursor.execute("""
                    INSERT INTO Fact_Motions (
                        idBuilding, idRoom, idSensor, idDate, idTime,
                        motion, light, temperature
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, 
                    building_id, room_id, sensor_id, date_id, time_id,
                    1 if motion_data.get('Motion', False) else 0,
                    motion_data.get('Light', 0),
                    motion_data.get('Temperature', 0.0)
                )
                inserted_count += 1
                
        except Exception as e:
            print(f"❌ Error processing motion data for room {room_name}: {e}")
            continue
    
    conn.commit()
    return inserted_count

def process_meteos(conn, data, date_id, time_id, building_id):
    """Process and insert meteo data."""
    cursor = conn.cursor()
    meteos_data = data.get('Meteos', {}).get('Meteo', {})
    sensor_id = get_or_create_sensor_id(conn, 'Meteo')
    
    inserted_count = 0
    
    for room_name, meteo_data in meteos_data.items():
        room_id = get_or_create_room_id(conn, room_name)
        
        try:
            # Check if record exists
            cursor.execute("""
                SELECT 1 FROM Fact_Meteos 
                WHERE idBuilding=? AND idRoom=? AND idSensor=? AND idDate=? AND idTime=?
            """, building_id, room_id, sensor_id, date_id, time_id)
            
            if cursor.fetchone():
                # Update existing record
                cursor.execute("""
                    UPDATE Fact_Meteos 
                    SET humidity=?, temperature=?, co2=?, batteryPercent=?, noise=?, pressure=?, absolutePressure=?
                    WHERE idBuilding=? AND idRoom=? AND idSensor=? AND idDate=? AND idTime=?
                """, 
                    meteo_data.get('Humidity', 0),
                    meteo_data.get('Temperature', 0.0),
                    meteo_data.get('Co2', 0),
                    meteo_data.get('BatteryPercent', 0),
                    meteo_data.get('Noise', 0),
                    meteo_data.get('Pressure', 0.0),
                    meteo_data.get('Absolutepressure', 0.0),
                    building_id, room_id, sensor_id, date_id, time_id
                )
            else:
                # Insert new record
                cursor.execute("""
                    INSERT INTO Fact_Meteos (
                        idBuilding, idRoom, idSensor, idDate, idTime,
                        humidity, temperature, co2, batteryPercent, noise, pressure, absolutePressure
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, 
                    building_id, room_id, sensor_id, date_id, time_id,
                    meteo_data.get('Humidity', 0),
                    meteo_data.get('Temperature', 0.0),
                    meteo_data.get('Co2', 0),
                    meteo_data.get('BatteryPercent', 0),
                    meteo_data.get('Noise', 0),
                    meteo_data.get('Pressure', 0.0),
                    meteo_data.get('Absolutepressure', 0.0)
                )
                inserted_count += 1
                
        except Exception as e:
            print(f"❌ Error processing meteo data for room {room_name}: {e}")
            continue
    
    conn.commit()
    return inserted_count

def process_humidities(conn, data, date_id, time_id, building_id):
    """Process and insert humidity data."""
    cursor = conn.cursor()
    humidities_data = data.get('Humidities', {})
    sensor_id = get_or_create_sensor_id(conn, 'Humidity')
    
    inserted_count = 0
    
    for room_name, humidity_data in humidities_data.items():
        room_id = get_or_create_room_id(conn, room_name)
        
        try:
            # Check if record exists
            cursor.execute("""
                SELECT 1 FROM Fact_Humidities 
                WHERE idBuilding=? AND idRoom=? AND idSensor=? AND idDate=? AND idTime=?
            """, building_id, room_id, sensor_id, date_id, time_id)
            
            if cursor.fetchone():
                # Update existing record
                cursor.execute("""
                    UPDATE Fact_Humidities 
                    SET temperature=?, humidity=?, devicePower=?
                    WHERE idBuilding=? AND idRoom=? AND idSensor=? AND idDate=? AND idTime=?
                """, 
                    humidity_data.get('Temperature', 0.0),
                    humidity_data.get('Humidity', 0.0),
                    humidity_data.get('Devicepower', 0),
                    building_id, room_id, sensor_id, date_id, time_id
                )
            else:
                # Insert new record
                cursor.execute("""
                    INSERT INTO Fact_Humidities (
                        idBuilding, idRoom, idSensor, idDate, idTime,
                        temperature, humidity, devicePower
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, 
                    building_id, room_id, sensor_id, date_id, time_id,
                    humidity_data.get('Temperature', 0.0),
                    humidity_data.get('Humidity', 0.0),
                    humidity_data.get('Devicepower', 0)
                )
                inserted_count += 1
                
        except Exception as e:
            print(f"❌ Error processing humidity data for room {room_name}: {e}")
            continue
    
    conn.commit()
    return inserted_count

def process_consumptions(conn, data, date_id, time_id, building_id):
    """Process and insert consumption data."""
    cursor = conn.cursor()
    consumptions_data = data.get('Consumptions', {})
    sensor_id = get_or_create_sensor_id(conn, 'Consumption')
    
    inserted_count = 0
    
    for room_name, consumption_data in consumptions_data.items():
        room_id = get_or_create_room_id(conn, room_name)
        
        try:
            # Check if record exists
            cursor.execute("""
                SELECT 1 FROM Fact_Consumptions 
                WHERE idBuilding=? AND idRoom=? AND idSensor=? AND idDate=? AND idTime=?
            """, building_id, room_id, sensor_id, date_id, time_id)
            
            if cursor.fetchone():
                # Update existing record
                cursor.execute("""
                    UPDATE Fact_Consumptions 
                    SET isValid1=?, isValid2=?, isValid3=?, current1=?, current2=?, current3=?,
                        power1=?, power2=?, power3=?, pf1=?, pf2=?, pf3=?,
                        voltage1=?, voltage2=?, voltage3=?, switch=?
                    WHERE idBuilding=? AND idRoom=? AND idSensor=? AND idDate=? AND idTime=?
                """, 
                    1 if consumption_data.get('IsValid1', False) else 0,
                    1 if consumption_data.get('IsValid2', False) else 0,
                    1 if consumption_data.get('IsValid3', False) else 0,
                    consumption_data.get('Current1', 0.0),
                    consumption_data.get('Current2', 0.0),
                    consumption_data.get('Current3', 0.0),
                    consumption_data.get('Power1', 0.0),
                    consumption_data.get('Power2', 0.0),
                    consumption_data.get('Power3', 0.0),
                    consumption_data.get('Pf1', 0.0),
                    consumption_data.get('Pf2', 0.0),
                    consumption_data.get('Pf3', 0.0),
                    consumption_data.get('Voltage1', 0.0),
                    consumption_data.get('Voltage2', 0.0),
                    consumption_data.get('Voltage3', 0.0),
                    1 if consumption_data.get('Switch', False) else 0,
                    building_id, room_id, sensor_id, date_id, time_id
                )
            else:
                # Insert new record
                cursor.execute("""
                    INSERT INTO Fact_Consumptions (
                        idBuilding, idRoom, idSensor, idDate, idTime,
                        isValid1, isValid2, isValid3, current1, current2, current3,
                        power1, power2, power3, pf1, pf2, pf3,
                        voltage1, voltage2, voltage3, switch
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, 
                    building_id, room_id, sensor_id, date_id, time_id,
                    1 if consumption_data.get('IsValid1', False) else 0,
                    1 if consumption_data.get('IsValid2', False) else 0,
                    1 if consumption_data.get('IsValid3', False) else 0,
                    consumption_data.get('Current1', 0.0),
                    consumption_data.get('Current2', 0.0),
                    consumption_data.get('Current3', 0.0),
                    consumption_data.get('Power1', 0.0),
                    consumption_data.get('Power2', 0.0),
                    consumption_data.get('Power3', 0.0),
                    consumption_data.get('Pf1', 0.0),
                    consumption_data.get('Pf2', 0.0),
                    consumption_data.get('Pf3', 0.0),
                    consumption_data.get('Voltage1', 0.0),
                    consumption_data.get('Voltage2', 0.0),
                    consumption_data.get('Voltage3', 0.0),
                    1 if consumption_data.get('Switch', False) else 0
                )
                inserted_count += 1
                
        except Exception as e:
            print(f"❌ Error processing consumption data for room {room_name}: {e}")
            continue
    
    conn.commit()
    return inserted_count

def process_json_file(file_path):
    """Process a single JSON file and insert data into the database."""
    try:
        # Get thread-local connection
        conn = get_thread_connection()

        # Load the JSON data
        data = load_json_file(file_path)
        
        # Parse date and time
        date_str = data.get('Datetime', '')
        time_str = data.get('Hours', '00:00')
        datetime_parts = parse_datetime(date_str, time_str)
        
        # Get or create dimension IDs
        date_id = get_or_create_date_id(conn, 
                                        datetime_parts['year'], 
                                        datetime_parts['month'], 
                                        datetime_parts['day'])
        
        time_id = get_or_create_time_id(conn, 
                                       datetime_parts['hour'], 
                                       datetime_parts['minute'])
        
        building_id = get_or_create_building_id(conn, data)
        
        # Process each sensor type
        results = {}
        
        if 'Plugs' in data:
            results['plugs'] = process_plugs(conn, data, date_id, time_id, building_id)
            
        if 'Doorswindows' in data:
            results['doorswindows'] = process_doors_windows(conn, data, date_id, time_id, building_id)
            
        if 'Motions' in data:
            results['motions'] = process_motions(conn, data, date_id, time_id, building_id)
            
        if 'Meteos' in data and 'Meteo' in data['Meteos']:
            results['meteos'] = process_meteos(conn, data, date_id, time_id, building_id)
            
        if 'Humidities' in data:
            results['humidities'] = process_humidities(conn, data, date_id, time_id, building_id)
            
        if 'Consumptions' in data:
            results['consumptions'] = process_consumptions(conn, data, date_id, time_id, building_id)
        
        # Print summary
        print(f"Processed file: {os.path.basename(file_path)}")
        for sensor_type, count in results.items():
            print(f"  - {sensor_type}: {count} records")
        
        mark_file_as_processed(file_path)

        return True
    except Exception as e:
        print(f"❌ Error processing file {file_path}: {e}")
        import traceback
        traceback.print_exc()
        return False

def find_json_files(directory):
    """Find all JSON files in the directory and its subdirectories."""
    return glob.glob(os.path.join(directory, '**', '*.json'), recursive=True)

def write_processed_files_to_csv():
    """Write all processed files to CSV at once (call this at the end)."""
    import pandas as pd
    
    # Get all items from the queue
    processed_files = []
    while not processed_files_queue.empty():
        processed_files.append(processed_files_queue.get())
    
    if not processed_files:
        print("No files were processed. Nothing to write to log.")
        return
    
    # Convert to DataFrame
    df = pd.DataFrame(processed_files)
    
    # If log file exists, append to it
    if os.path.exists(PROCESSED_LOG):
        try:
            existing_df = pd.read_csv(PROCESSED_LOG)
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            # Drop duplicates if any
            combined_df = combined_df.drop_duplicates(subset=['filepath'])
            combined_df.to_csv(PROCESSED_LOG, index=False)
        except Exception as e:
            print(f"Warning: Could not append to existing log: {e}")
            # Just write the new records
            df.to_csv(PROCESSED_LOG, index=False)
    else:
        # Create new log file
        df.to_csv(PROCESSED_LOG, index=False)
    
    print(f"Wrote {len(processed_files)} processed files to log.")

def process_all_json_files(directory):
    """Process all JSON files in the directory and its subdirectories using multiple threads."""
    try:
        # Get list of already processed files (do this once before threading)
        processed_files_df = get_processed_files()
        print(f"Found {len(processed_files_df)} processed files in log: {PROCESSED_LOG}")
        
        # Find all JSON files
        json_files = find_json_files(directory)
        print(f"Found {len(json_files)} JSON files in {directory}")
        
        # Filter out already processed files
        files_to_process = []
        for file_path in json_files:
            if should_process_file(file_path, processed_files_df):
                files_to_process.append(file_path)
        
        print(f"Found {len(files_to_process)} files that need processing")
        
        # If no files to process, exit
        if not files_to_process:
            print("No new files to process. Exiting.")
            return
        
        # Get the number of workers from config
        max_workers = Config.MAX_WORKERS
        print(f"Using {max_workers} worker threads for processing")
        
        # Process files using thread pool
        processed_count = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(process_json_file, file_path): file_path 
                for file_path in files_to_process
            }
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    if future.result():
                        processed_count += 1
                        print(f"Progress: {processed_count}/{len(files_to_process)} files processed")
                except Exception as exc:
                    print(f"❌ File {os.path.basename(file_path)} generated an exception: {exc}")
        
        write_processed_files_to_csv()
        print(f"✅ Processed {processed_count} out of {len(json_files)} JSON files")
        
    except Exception as e:
        print(f"❌ Error processing JSON files: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # Close any open connections
        if hasattr(thread_local, "connection"):
            try:
                thread_local.connection.close()
                print("Closed database connection")
            except:
                pass

def main():
    """Main function."""
    try:
        start_time = datetime.now()
        print(f"Started processing at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Script is running from: {SCRIPT_DIR}")
        print(f"Processing log will be stored at: {PROCESSED_LOG}")
        print(f"Data directory: {Config.SENSORS_DATA_DIR}")
        print(f"Using {Config.MAX_WORKERS} worker threads from config")
        
        # Process all JSON files
        process_all_json_files(Config.SENSORS_DATA_DIR)
        
        end_time = datetime.now()
        duration = end_time - start_time
        print(f"Finished processing at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total processing time: {duration}")
        
    except Exception as e:
        try:
            write_processed_files_to_csv()
        except Exception as csv_e:
            print(f"❌ Error writing processed files to CSV: {csv_e}")
        print(f"❌ Error in main function: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()