# Clean Storage Project Structure

## Directory Structure

```
CleanStorage/
├── Data/
│   ├── Sensors/
│   │   ├── Appartment_1/
│   │   │   ├── 2023/
│   │   │   │   ├── 06/
│   │   │   │   │   ├── 01/
│   │   │   │   │   │   ├── (Cleaned JSON files for apartment 1)
│   │   │   │   │   │   └── ...
│   │   │   │   │   └── ...
│   │   │   │   └── ...
│   │   │   └── ...
│   │   ├── Appartment_2/
│   │   │   ├── 2023/
│   │   │   │   ├── 06/
│   │   │   │   │   ├── 01/
│   │   │   │   │   │   ├── (Cleaned JSON files for apartment 2)
│   │   │   │   │   │   └── ...
│   │   │   │   │   └── ...
│   │   │   │   └── ...
│   │       └── ...
│   ├── Weather/
│   │   ├── (Cleaned CSV weather data files)
│   │   └── ...
├── Scripts/
│   ├── sensors_data_clean.py
│   ├── weather_data_clean.py
│   ├── config.py
│   ├── config.ini
│   └── ...
├── Logs/
│   ├── Sensors/
│   │   ├── sensor_2025-03-05_10-37-32.log
│   │   ├── sensor_2025-03-05_11-45-12.log
│   │   └── ...
│   ├── Weather/
│   │   ├── weather_2025-03-05_10-38-15.log
│   │   ├── weather_2025-03-05_11-42-33.log
│   │   └── ...
│   └── ...
└── ...
```

## Key Components

### Data Storage

1. **Cleaned Sensors Data**:
   - Maintains the same apartment/year/month/day hierarchy as RawStorage
   - Contains cleaned and standardized JSON files
   - Data has standardized date formats, PascalCase keys, and boolean conversions

2. **Cleaned Weather Data**:
   - Contains processed CSV files
   - Data filtered for specific locations (Sion, Visp)
   - Standardized timestamp formats with separate Hour column
   - Invalid values (-99999) replaced with 0

### Logs Directory

A dedicated top-level Logs directory contains organized logging information:

1. **Sensors Logs**:
   - Processing logs with timestamps in the format `sensor_YYYY-MM-DD_HH-MM-SS.log`
   - Contains detailed information about file processing, thread activities, and errors

2. **Weather Logs**:
   - Processing logs with timestamps in the format `weather_YYYY-MM-DD_HH-MM-SS.log`
   - Contains information about CSV processing, row counts, filtering results

### Scripts

The Scripts directory contains the cleaning scripts that populate the CleanStorage structure:

1. **Data Cleaning**:
   - `sensors_data_clean.py`: Multi-threaded processing of sensor JSON data
   - `weather_data_clean.py`: Multi-threaded processing of weather CSV data

2. **Configuration**:
   - `config.py`: Configuration module that defines paths and settings
   - `config.ini`: Contains path definitions and worker thread counts

## Data Flow and Processing

1. **Input**: Scripts read raw data from the RawStorage structure
2. **Processing**: Data is cleaned, standardized, and validated
3. **Output**: Processed data is stored in the CleanStorage structure
4. **Logging**: Detailed logs of the processing are stored in the Logs directory