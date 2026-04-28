# RawStorage Project Structure

## Directory Structure

```
RawStorage/
├── Data/
│   ├── Sensors/
│   │   ├── Apartment_1/
│   │   │   ├── 2023/
│   │   │   │   ├── 06/
│   │   │   │   │   ├── 01/
│   │   │   │   │   │   ├── (JSON files for apartment 1 sensors)
│   │   │   │   │   │   └── ...
│   │   │   │   │   └── ...
│   │   │   │   └── ...
│   │   │   └── ...
│   │   ├── Apartment_2/
│   │   │   ├── 2023/
│   │   │   │   ├── 06/
│   │   │   │   │   ├── 01/
│   │   │   │   │   │   ├── (JSON files for apartment 2 sensors)
│   │   │   │   │   │   └── ...
│   │   │   │   │   └── ...
│   │   │   │   └── ...
│   │   │   └── ...
│   │   └── ...
│   ├── Weather/
│   │   ├── (CSV weather data files)
│   │   └── ...
│   └── ...
├── Scripts/
│   ├── sensor_data_fetcher.py
│   ├── weather_data_fetcher.py
│   ├── config.py
│   ├── config.ini
│   └── ...
└── ...
```

## Key Components

### Data Storage

1. **Sensors Data**:
   - Organized by apartment (Apartment_1, Apartment_2)
   - Further organized by year/month/day hierarchy
   - Contains JSON files with sensor readings

2. **Weather Data**:
   - Stored as CSV files
   - Contains weather measurements for locations like Sion and Visp

### Scripts

1. **Data Fetching**:
   - `sensor_data_fetcher.py`: Retrieves sensor data via SMB with multi-threading
   - `weather_data_fetcher.py`: Retrieves weather data via SFTP with multi-threading

2. **Configuration**:
   - `config.py`: Central configuration module
   - `config.ini`: Configuration settings for paths and thread counts

## Data Flow

1. **Collection**: Data is fetched from remote sources using the fetcher scripts
2. **Storage**: Raw data is stored in the appropriate directories
3. **Organization**: Processed data maintains the same hierarchical structure