import json
import pyodbc
import os
from datetime import datetime
from typing import List, Dict, Any
from config import Config

# Load configuration
Config.load()

# Database connection configuration using secure credentials
DRIVER = '{ODBC Driver 17 for SQL Server}'

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

def load_json_file(file_path: str) -> Any:
    """Load data from a JSON file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except Exception as e:
        print(f"❌ Error loading JSON file {file_path}: {e}")
        raise

def load_building_types(conn, building_types_data: List[Dict]):
    """Load building type data for reference."""
    cursor = conn.cursor()
    print("Loading building types...")
    
    # We only need the building types for reference when inserting buildings
    building_types = {}
    for bt in building_types_data:
        building_types[bt['idBuildingType']] = bt['type']
    
    print(f"Loaded {len(building_types)} building types for reference")
    return building_types

def load_buildings(conn, buildings_data: List[Dict], building_types: Dict):
    """Load building data into DimBuilding table."""
    cursor = conn.cursor()
    print("Loading buildings...")
    
    inserted_count = 0
    
    for building in buildings_data:
        # Get building type from reference dict
        building_type = building_types.get(building['idBuildingType'], "Unknown")
        
        # Check if building already exists
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM DimBuilding WHERE houseName=?)
            INSERT INTO DimBuilding (buildingType, houseName, latitude, longitude, adress, npa, city, nbPeople, isHeatingOn)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, building['houseName'], building_type, building['houseName'], 
            building['latitude'], building['longitude'], building['address'], building['npa'], building['city'], 
            building['nbPeople'], building['isHeatingOn'])
        
        conn.commit()
        inserted_count += 1
    
    print(f"Processed {inserted_count} buildings in DimBuilding")

def load_rooms(conn, rooms_data: List[Dict]):
    """Load room data into DimRoom table."""
    cursor = conn.cursor()
    print("Loading rooms...")
    
    inserted_count = 0
    
    for room in rooms_data:
        if 'roomName' not in room:
            print(f"❌ Missing 'roomName' in room: {room}")
            continue
        
        # Fix room names: change Bdroom to Bedroom and Bhroom to Bathroom
        fixed_room_name = room['roomName']
        fixed_room_name = fixed_room_name.replace('Bdroom', 'Bedroom')
        fixed_room_name = fixed_room_name.replace('Bhroom', 'Bathroom')
        
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM DimRoom WHERE roomName=?)
            INSERT INTO DimRoom (roomName)
            VALUES (?)
        """, fixed_room_name, fixed_room_name)
        
        if fixed_room_name != room['roomName']:
            print(f"Changed room name from '{room['roomName']}' to '{fixed_room_name}'")
        
        inserted_count += 1
    
    conn.commit()
    print(f"Processed {inserted_count} rooms in DimRoom")

def load_sensors(conn, sensors_data: List[Dict]):
    """Load sensor data into DimSensor table."""
    cursor = conn.cursor()
    print("Loading sensors...")
    
    inserted_count = 0
    
    for sensor in sensors_data:
        if 'sensorType' not in sensor:
            print(f"❌ Missing 'sensorType' in sensor: {sensor}")
            continue
            
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM DimSensor WHERE sensorType=?)
            INSERT INTO DimSensor (sensorType)
            VALUES (?)
        """, sensor['sensorType'], sensor['sensorType'])
        
        inserted_count += 1
    
    conn.commit()
    print(f"Processed {inserted_count} sensors in DimSensor")

def main():
    """Main function to load all data."""
    try:
        # Use the path from Config
        print(f"Loading JSON files from {Config.STATIC_DATA_DIR}...")
        
        # Load JSON files using the path from Config
        building_types_data = load_json_file(os.path.join(Config.STATIC_DATA_DIR, 'buildingType.json'))
        buildings_data = load_json_file(os.path.join(Config.STATIC_DATA_DIR, 'buildings.json'))
        rooms_data = load_json_file(os.path.join(Config.STATIC_DATA_DIR, 'rooms.json'))
        sensors_data = load_json_file(os.path.join(Config.STATIC_DATA_DIR, 'sensors.json'))
        
        # Connect to the database
        print("Connecting to database...")
        conn = connect_to_db()
        
        # Load dimension data
        building_types = load_building_types(conn, building_types_data)
        load_buildings(conn, buildings_data, building_types)
        load_rooms(conn, rooms_data)
        load_sensors(conn, sensors_data)
        
        # Close connection
        conn.close()
        print("✅ Data successfully inserted into SQL Server!")
        
    except Exception as e:
        print(f"❌ Error in main function: {e}")

if __name__ == "__main__":
    main()