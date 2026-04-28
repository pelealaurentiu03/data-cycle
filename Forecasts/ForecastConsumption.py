import os
import json
import glob
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import joblib
from config import Config

config = Config.load()
base_dir = str(config.SENSORS_DATA_DIR)
weather_dir = str(config.WEATHER_DATA_DIR)

# =============================================
# =============== Extract and Combine Data
# =============================================


def extract_consumption_data(json_data, apartment_id):
    data = {}
    
    data['DateTime'] = json_data.get('Datetime', '')
    data['Hours'] = json_data.get('Hours', '')
    data['User'] = json_data.get('User', '')
    data['ApartmentID'] = apartment_id
    
    # Process Consumptions
    if 'Consumptions' in json_data:
        for location, consumption_data in json_data['Consumptions'].items():
            for key, value in consumption_data.items():
                data[f'{location}_Consumption_{key}'] = value
    
    return data

def find_latest_data_dates(base_dir, apartment_id, year):
    apartment_year_path = os.path.join(base_dir, apartment_id, year)
    
    if not os.path.exists(apartment_year_path):
        print(f"Path not found: {apartment_year_path}")
        return []
    
    # Get all available months
    months = []
    for item in os.listdir(apartment_year_path):
        month_path = os.path.join(apartment_year_path, item)
        if os.path.isdir(month_path) and item.isdigit() and 1 <= int(item) <= 12:
            months.append(item)
    
    months.sort(reverse=True)  # Sort in descending order to get latest months first
    
    all_dates = []
    for month in months:
        month_path = os.path.join(apartment_year_path, month)
        
        # Look for day folders first
        days = []
        for item in os.listdir(month_path):
            day_path = os.path.join(month_path, item)
            if os.path.isdir(day_path) and item.isdigit() and 1 <= int(item) <= 31:
                days.append(item)
        
        # If no day folders, try to infer days from filenames
        if not days:
            files = os.listdir(month_path)
            for file in files:
                if file.endswith('.json'):
                    # Try to extract day from filename (format: DD.MM.YYYY*.json)
                    parts = file.split('.')
                    if len(parts) >= 3 and parts[0].isdigit() and 1 <= int(parts[0]) <= 31:
                        days.append(parts[0])
        
        days = list(set(days))  # Remove duplicates
        days.sort(reverse=True)  # Sort in descending order
        
        for day in days:
            date_str = f"{year}-{month}-{day}"
            try:
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                all_dates.append(date_obj)
            except ValueError:
                pass  # Skip invalid dates
    
    all_dates.sort(reverse=True)  # Sort all dates in descending order
    return all_dates

def extract_data_for_date_range(base_dir, apartment_id, year, date_range):
    all_data = []
    
    for current_date in date_range:
        # Extract month and day
        month = f"{current_date.month:02d}"
        day = current_date.day
        day_str = f"{day:02d}"
        
        # Create path to the data
        month_dir = os.path.join(base_dir, apartment_id, year, month)
        
        if os.path.exists(month_dir):
            # Check if there's a day subfolder
            day_dir = os.path.join(month_dir, day_str)
            if os.path.exists(day_dir):
                # Look in the day folder
                pattern = os.path.join(day_dir, "*.json")
                json_files = glob.glob(pattern)
                print(f"Found {len(json_files)} files for {current_date.strftime('%Y-%m-%d')} in subfolder")
            else:
                # Look directly in the month folder
                pattern = os.path.join(month_dir, f"{day_str}.{month}.{year}*.json")
                json_files = glob.glob(pattern)
                print(f"Found {len(json_files)} files for {current_date.strftime('%Y-%m-%d')} in month folder")
            
            # Process all JSON files for this day
            for json_file in json_files:
                try:
                    with open(json_file, 'r') as f:
                        data = json.load(f)
                    
                    # Extract consumption data
                    consumption_data = extract_consumption_data(data, apartment_id)
                    if consumption_data and any(key for key in consumption_data.keys() if 'Consumption' in key):
                        all_data.append(consumption_data)
                except Exception as e:
                    print(f"Error processing {json_file}: {e}")
    
    return all_data

def extract_latest_weeks_consumption_data(base_dir, apartment_id, year, num_weeks=2):
    print(f"Extracting the last {num_weeks} weeks of available data for {apartment_id}...")
    
    # Find the latest dates in the dataset
    available_dates = find_latest_data_dates(base_dir, apartment_id, year)
    
    if not available_dates:
        print(f"No dates found for {apartment_id} in {year}")
        return None
    
    # Get the most recent date
    latest_date = available_dates[0]
    print(f"Latest available date: {latest_date.strftime('%Y-%m-%d')}")
    
    # Calculate the start date (N weeks before the latest date)
    num_days = num_weeks * 7
    start_date = latest_date - timedelta(days=num_days-1)  # -1 to include the start day
    
    # Create a list of dates in the range
    date_range = []
    current = start_date
    while current <= latest_date:
        date_range.append(current)
        current += timedelta(days=1)
    
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {latest_date.strftime('%Y-%m-%d')} ({len(date_range)} days)")
    
    # Extract data for the date range
    all_data = extract_data_for_date_range(base_dir, apartment_id, year, date_range)
    
    # If we have data, create a DataFrame
    if all_data:
        df = pd.DataFrame(all_data)
        
        # Convert DateTime to datetime objects
        df['DateTime'] = pd.to_datetime(df['DateTime'], format='%d/%m/%Y', errors='coerce')
        
        # Extract hour from Hours column (format: 'HH:MM')
        if 'Hours' in df.columns:
            df['Hour'] = df['Hours'].str.split(':').str[0].astype(int)
        else:
            df['Hour'] = df['DateTime'].dt.hour
        
        # Add time-based features
        df['Date'] = df['DateTime'].dt.strftime('%d/%m/%Y')
        df['DayOfWeek'] = df['DateTime'].dt.dayofweek  # 0 is Monday, 6 is Sunday
        df['IsWeekend'] = df['DayOfWeek'].isin([5, 6]).astype(int)  # 1 for weekends
        
        # Filter to keep only the required columns for each room
        # Assuming 'House' is the room you're interested in
        house_cols = [col for col in df.columns if 'House' in col and 'Consumption' in col]
        keep_cols = ['DateTime', 'Date', 'Hour', 'Hours', 'DayOfWeek', 'IsWeekend', 
                     'ApartmentID', 'User'] + house_cols
        
        # Keep only columns that exist in the DataFrame
        keep_cols = [col for col in keep_cols if col in df.columns]
        
        # Filter the DataFrame
        df = df[keep_cols].copy()
        
        # Sort by date and time
        df = df.sort_values(['DateTime', 'Hour'])
        
        print(f"Extracted {len(df)} records for {apartment_id}")
        return df
    
    print(f"No data found for {apartment_id}")
    return None

def save_latest_weeks_consumption_data(apartment_ids, base_dir, year, num_weeks=2):
    results = {}
    
    for apartment_id in apartment_ids:
        # Extract data
        df = extract_latest_weeks_consumption_data(base_dir, apartment_id, year, num_weeks)
        
        if df is not None and not df.empty:
            # Save to a single CSV
            output_filename = f"{apartment_id}_latest_{num_weeks}weeks_consumption.csv"
            df.to_csv(output_filename, index=False)
            print(f"Saved latest {num_weeks} weeks of consumption data for {apartment_id} to {output_filename}")
            print(f"DataFrame shape: {df.shape}")
            results[apartment_id] = df
        else:
            print(f"No data found for {apartment_id}")
            results[apartment_id] = None
    
    return results

# Usage
year = "2023"
apartment_ids = ["Apartment_1", "Apartment_2"]

# Extract and save the latest 2 weeks of available data
apartment_data = save_latest_weeks_consumption_data(apartment_ids, base_dir, year, num_weeks=2)


# =============================================
# =============== Fix for duplicates, missing values, outliers
# =============================================


def remove_rows_with_missing_values(df, threshold=None, subset=None):

    # Make a copy of the dataframe
    df_clean = df.copy()
    
    # If subset is specified, check only those columns
    if subset is not None:
        # Ensure all specified columns exist in the dataframe
        subset = [col for col in subset if col in df.columns]
        if not subset:
            print("None of the specified columns exist in the dataframe.")
            return df_clean
    
    # Count original rows
    original_rows = len(df_clean)
    
    if threshold is not None:
        if not 0 <= threshold <= 1:
            raise ValueError("Threshold must be between 0.0 and 1.0")
        
        # Calculate missing values ratio for each row
        if subset:
            missing_ratio = df_clean[subset].isnull().mean(axis=1)
        else:
            missing_ratio = df_clean.isnull().mean(axis=1)
        
        # Remove rows with missing ratio > threshold
        rows_to_drop = missing_ratio[missing_ratio > threshold].index
        if len(rows_to_drop) > 0:
            df_clean = df_clean.drop(index=rows_to_drop)
            rows_removed = len(rows_to_drop)
            print(f"Removed {rows_removed} rows with > {threshold*100}% missing values "
                  f"({rows_removed/original_rows*100:.2f}% of data)")
        else:
            print(f"No rows with > {threshold*100}% missing values found.")
    else:
        # Remove rows with any missing values
        if subset:
            df_clean = df_clean.dropna(subset=subset)
        else:
            df_clean = df_clean.dropna()
        
        # Calculate number of rows removed
        rows_removed = original_rows - len(df_clean)
        if rows_removed > 0:
            print(f"Removed {rows_removed} rows with missing values "
                  f"({rows_removed/original_rows*100:.2f}% of data)")
        else:
            print("No rows with missing values found.")
    
    return df_clean

def remove_duplicates(df):

    # Count duplicates before removal
    dup_count = df.duplicated().sum()
    
    # If we have time-based data, check for time-based duplicates
    if 'DateTime' in df.columns and 'Hours' in df.columns:
        time_dup_count = df.duplicated(subset=['DateTime', 'Hours']).sum()
        if time_dup_count > 0:
            print(f"Found {time_dup_count} duplicate time entries. Removing...")
            return df.drop_duplicates(subset=['DateTime', 'Hours'], keep='first')
    
    # If no time-based duplicates but we have exact duplicates
    if dup_count > 0:
        print(f"Found {dup_count} exact duplicate rows. Removing...")
        return df.drop_duplicates()
    
    print("No duplicates found.")
    return df

def handle_outliers(df, method='iqr', columns=None, threshold=1.5):

    # Make a copy of the dataframe
    df_clean = df.copy()
    
    # Select columns to check for outliers
    if columns is None:
        columns = df.select_dtypes(include=['int64', 'float64']).columns
        # Exclude columns ending with _Motion
        columns = [col for col in columns if not col.endswith('_Motion')]
    else:
        # Ensure all specified columns exist and are numeric
        columns = [col for col in columns if col in df.columns and
                   pd.api.types.is_numeric_dtype(df[col]) and
                   not col.endswith('_Motion')]
    
    outlier_info = {}
    
    # Process each column
    for col in columns:
        # Skip columns with all identical values
        if df[col].nunique() <= 1:
            continue
        
        # Calculate boundaries for outliers based on selected method
        if method == 'iqr':
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            
            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR
            
            # Identify outliers
            outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
            outlier_count = len(outliers)
            
            if outlier_count > 0:
                outlier_percentage = outlier_count / len(df) * 100
                outlier_info[col] = {
                    'count': outlier_count,
                    'percentage': outlier_percentage,
                    'bounds': (lower_bound, upper_bound)
                }
                
                # Handle outliers based on specified method
                if method == 'iqr':
                    # Cap values at the boundaries
                    df_clean.loc[df_clean[col] < lower_bound, col] = lower_bound
                    df_clean.loc[df_clean[col] > upper_bound, col] = upper_bound
        
        elif method == 'zscore':
            # Calculate z-scores
            mean = df[col].mean()
            std = df[col].std()
            z_scores = (df[col] - mean) / std
            
            # Identify outliers (|z| > threshold)
            outliers = df[abs(z_scores) > threshold]
            outlier_count = len(outliers)
            
            if outlier_count > 0:
                outlier_percentage = outlier_count / len(df) * 100
                lower_bound = mean - threshold * std
                upper_bound = mean + threshold * std
                
                outlier_info[col] = {
                    'count': outlier_count,
                    'percentage': outlier_percentage,
                    'bounds': (lower_bound, upper_bound)
                }
                
                # Cap values at z-score boundaries
                df_clean.loc[df_clean[col] < lower_bound, col] = lower_bound
                df_clean.loc[df_clean[col] > upper_bound, col] = upper_bound
        
        elif method == 'remove':
            # For this method, we'll collect all outliers first, then remove rows at the end
            if col not in outlier_info:
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                
                lower_bound = Q1 - threshold * IQR
                upper_bound = Q3 + threshold * IQR
                
                # Identify outliers
                outlier_mask = (df[col] < lower_bound) | (df[col] > upper_bound)
                outliers = df[outlier_mask]
                outlier_count = len(outliers)
                
                if outlier_count > 0:
                    outlier_percentage = outlier_count / len(df) * 100
                    outlier_info[col] = {
                        'count': outlier_count,
                        'percentage': outlier_percentage,
                        'bounds': (lower_bound, upper_bound),
                        'mask': outlier_mask
                    }
    
    # If we're removing outlier rows, we need to do it once for all columns
    if method == 'remove' and outlier_info:
        # Combine all outlier masks
        combined_mask = pd.Series(False, index=df.index)
        for col, info in outlier_info.items():
            combined_mask = combined_mask | info['mask']
        
        # Remove rows with outliers
        df_clean = df_clean[~combined_mask]
        rows_removed = sum(combined_mask)
        print(f"Removed {rows_removed} rows containing outliers ({rows_removed/len(df)*100:.2f}% of data)")
    
    # Report on outliers found and handled
    if outlier_info:
        print(f"Found and handled outliers in {len(outlier_info)} columns using {method} method:")
        
        # Sort by percentage to show most affected columns first
        sorted_outliers = sorted(outlier_info.items(), 
                                key=lambda x: x[1]['percentage'] if 'percentage' in x[1] else 0, 
                                reverse=True)
        
        for col, info in sorted_outliers[:10]:  # Show top 10
            if method != 'remove':
                print(f"  - {col}: {info['count']} outliers ({info['percentage']:.2f}%) - capped at [{info['bounds'][0]:.2f}, {info['bounds'][1]:.2f}]")
    else:
        print("No outliers found or handled.")
    
    if len(outlier_info) > 10:
        print(f"  ... and {len(outlier_info) - 10} more columns")
    
    return df_clean, outlier_info

def clean_data(filepath, outlier_method='iqr', outlier_threshold=1.5):

    print(f"Cleaning data from: {filepath}")
    
    # Load the data
    df = pd.read_csv(filepath)
    print(f"Initial shape: {df.shape[0]} rows, {df.shape[1]} columns")
    
    # 1. Remove rows missing values
    df_no_missing = remove_rows_with_missing_values(df)
    print(f"After removing rows with missing values: {df_no_missing.shape[0]} rows, {df_no_missing.shape[1]} columns")
    
    # 2. Remove duplicates
    df_no_duplicates = remove_duplicates(df_no_missing)
    print(f"After removing duplicates: {df_no_duplicates.shape[0]} rows, {df_no_duplicates.shape[1]} columns")
    
    # 3. Handle outliers
    df_clean, outlier_info = handle_outliers(
        df_no_duplicates, 
        method=outlier_method,
        threshold=outlier_threshold
    )
    print(f"Final shape after cleaning: {df_clean.shape[0]} rows, {df_clean.shape[1]} columns")
    
    return df_clean


df_clean = clean_data('Apartment_1_latest_2weeks_consumption.csv')
df_clean.to_csv('Apartment_1_latest_2weeks_consumption.csv', index=False)

df_clean = clean_data('Apartment_2_latest_2weeks_consumption.csv')
df_clean.to_csv('Apartment_2_latest_2weeks_consumption.csv', index=False)


# =============================================
# =============== Removing Columns
# =============================================


def clean_apartment_data(file_path):

    # Load the dataset
    df = pd.read_csv(file_path)
    
    # List of columns to keep
    columns_to_keep = [
        'DateTime',
        'Hour',
        'DayOfWeek',
        'IsWeekend',
        'House_Consumption_TotalPower' # Main target variable
    ]
    
    # Keep only the useful columns
    df_cleaned = df[columns_to_keep]
    
    # Save the cleaned dataset
    df_cleaned.to_csv(file_path, index=False)


clean_apartment_data('Apartment_1_latest_2weeks_consumption.csv')
clean_apartment_data('Apartment_2_latest_2weeks_consumption.csv')


# =============================================
# =============== Transform to hourly data
# =============================================


def aggregate_hourly_data(file_path):

    # Load the cleaned dataset
    df = pd.read_csv(file_path)
    
    # Convert DateTime to proper datetime format
    df['DateTime'] = pd.to_datetime(df['DateTime'])
    
    # Create a new column for the date without time
    df['Date'] = df['DateTime'].dt.date
    
    # Group by date and hour
    hourly_data = df.groupby(['Date', 'Hour']).agg(
        TotalPower_Sum=('House_Consumption_TotalPower', 'sum'),
        DayOfWeek=('DayOfWeek', 'first'),   # All entries in an hour have the same DayOfWeek
        IsWeekend=('IsWeekend', 'first'),   # All entries in an hour have the same IsWeekend
        EntryCount=('House_Consumption_TotalPower', 'count')  # Count entries per hour
    ).reset_index()
    
    # Format Hours as HH:00
    hourly_data['Hours'] = hourly_data['Hour'].apply(lambda x: f"{x:02d}:00")
    
    # Rename Date column to DateTime to keep the original format
    hourly_data = hourly_data.rename(columns={'Date': 'DateTime'})
    
    # Reorder columns to have DateTime and Hours at the beginning
    columns_order = ['DateTime', 'Hours', 'Hour', 'DayOfWeek', 'IsWeekend',
                    'TotalPower_Sum', 'EntryCount']
    hourly_data = hourly_data[columns_order]
    
    # Check for hours with fewer than expected entries (assuming 1 entry per minute)
    expected_entries = 60  # 60 minutes per hour
    missing_data = hourly_data[hourly_data['EntryCount'] < expected_entries]
    print(f"\nHours with fewer than {expected_entries} entries (potentially missing data):")
    print(missing_data[['DateTime', 'Hours', 'EntryCount']])
    
    # Calculate average entries per hour
    avg_entries = hourly_data['EntryCount'].mean()
    print(f"\nAverage entries per hour: {avg_entries:.2f}")
    
    # Save the hourly aggregated data
    hourly_data.to_csv(file_path, index=False)


aggregate_hourly_data('Apartment_1_latest_2weeks_consumption.csv')
aggregate_hourly_data('Apartment_2_latest_2weeks_consumption.csv')


# =============================================
# =============== Adding weather data
# =============================================


def find_closest_weather_file(sensor_date, weather_files):
    """
    Find the closest weather prediction file to a given sensor date
    """
    closest_file = None
    min_diff = float('inf')
    
    sensor_date_only = sensor_date.date() if hasattr(sensor_date, 'date') else sensor_date
    
    for file in weather_files:
        try:
            # Extract date from filename (format: Pred_2023-01-01.csv)
            date_str = os.path.basename(file).split('_')[1].split('.')[0]
            file_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            
            # Calculate day difference
            diff = abs((file_date - sensor_date_only).days)
            
            # Update closest file if this one is closer
            if diff < min_diff:
                min_diff = diff
                closest_file = file
        except Exception as e:
            print(f"Error parsing date from file {file}: {e}")
    
    return closest_file

def find_closest_time_prediction(sensor_date, sensor_time, weather_df, site, measurement, debug=False):
    """
    Find the weather prediction with the closest time for a specific site and measurement
    """
    # Convert sensor_date to string format used in weather file
    sensor_date_str = sensor_date.strftime('%Y-%m-%d')
    
    if debug:
        print(f"\nDEBUG - Looking for: Date={sensor_date_str}, Time={sensor_time}, Site={site}, Measurement={measurement}")
        print(f"Weather file shape: {weather_df.shape}")
        print(f"Weather file columns: {weather_df.columns.tolist()}")
        print(f"Sample of weather data:\n{weather_df.head(3)}")
        print(f"Weather file date format example: {weather_df['Time'].iloc[0] if not weather_df.empty else 'No data'}")
        print(f"Checking if contains {sensor_date_str} in {weather_df['Time'].iloc[0] if not weather_df.empty else 'No data'}")
    
    # Check if 'Time' column exists
    if 'Time' not in weather_df.columns:
        if debug:
            print("No 'Time' column found in weather data")
        return np.nan, ""
        
    # Check if date format matches exactly or needs conversion
    date_format_exact = weather_df['Time'].astype(str).str.contains(sensor_date_str).any()
    date_format_with_dashes = weather_df['Time'].astype(str).str.contains(sensor_date_str.replace('-', '/')).any()
    reversed_date = f"{sensor_date_str.split('-')[2]}-{sensor_date_str.split('-')[1]}-{sensor_date_str.split('-')[0]}"
    date_format_reversed = weather_df['Time'].astype(str).str.contains(reversed_date).any()
    
    if debug:
        print(f"Date format matches - Exact: {date_format_exact}, With slashes: {date_format_with_dashes}, Reversed: {date_format_reversed}")
        print(f"Reversed date format: {reversed_date}")
    
    # Try different date formats
    if date_format_exact:
        date_filtered = weather_df[weather_df['Time'].astype(str).str.contains(sensor_date_str)]
    elif date_format_with_dashes:
        date_filtered = weather_df[weather_df['Time'].astype(str).str.contains(sensor_date_str.replace('-', '/'))]
    elif date_format_reversed:
        date_filtered = weather_df[weather_df['Time'].astype(str).str.contains(reversed_date)]
    else:
        # If no exact match, try with contains
        day = sensor_date_str.split('-')[2]
        month = sensor_date_str.split('-')[1]
        date_filtered = weather_df[weather_df['Time'].astype(str).str.contains(f"{day}-{month}", na=False)]
    
    if debug:
        print(f"After date filter: {len(date_filtered)} rows")
        if not date_filtered.empty:
            print(f"Sample after date filter:\n{date_filtered.head(3)}")
    
    # Check if 'Site' and 'Measurement' columns exist
    if 'Site' not in weather_df.columns or 'Measurement' not in weather_df.columns:
        if debug:
            print("Missing required columns 'Site' or 'Measurement' in weather data")
        return np.nan, ""
    
    site_measurement_filtered = date_filtered[
        (date_filtered['Site'] == site) & 
        (date_filtered['Measurement'] == measurement)
    ]
    
    if debug:
        print(f"After site/measurement filter: {len(site_measurement_filtered)} rows")
        if not site_measurement_filtered.empty:
            print(f"Sample after site/measurement filter:\n{site_measurement_filtered.head(3)}")
    
    if site_measurement_filtered.empty:
        if debug:
            print("No matches found with exact criteria, trying without date filter")
        
        # Try without the exact date match
        site_measurement_filtered = weather_df[
            (weather_df['Site'] == site) & 
            (weather_df['Measurement'] == measurement)
        ]
        
        if site_measurement_filtered.empty:
            if debug:
                print("Still no matches found")
            return np.nan, ""
    
    # Make a copy to avoid warnings
    filtered_df = site_measurement_filtered.copy()
    
    # Check if 'Hour' column exists
    if 'Hour' not in filtered_df.columns:
        if debug:
            print("No 'Hour' column found in weather data")
        return np.nan, ""
    
    # Extract hour and minute from sensor time
    try:
        if isinstance(sensor_time, str) and ':' in sensor_time:
            hour, minute = map(int, sensor_time.split(':')[:2])
        else:
            hour, minute = 0, 0
    except:
        hour, minute = 0, 0
    
    # Calculate minutes since midnight for sensor time
    sensor_minutes = hour * 60 + minute
    
    if debug:
        print(f"Sensor time: {hour}:{minute} ({sensor_minutes} minutes from midnight)")
    
    # Extract hours from 'Hour' column in weather data
    filtered_df['hour_only'] = filtered_df['Hour'].astype(str).apply(
        lambda x: int(x.split(':')[0]) if isinstance(x, str) and ':' in x else 0
    )
    
    # Calculate minutes since midnight for weather times
    filtered_df['minutes_from_midnight'] = filtered_df['hour_only'] * 60
    
    if debug:
        print(f"Hours in weather data: {filtered_df['hour_only'].tolist()}")
        print(f"Minutes from midnight: {filtered_df['minutes_from_midnight'].tolist()}")
    
    # Calculate time difference
    filtered_df['time_diff'] = abs(filtered_df['minutes_from_midnight'] - sensor_minutes)
    
    if debug:
        print(f"Time differences: {filtered_df['time_diff'].tolist()}")
    
    # Find entries with the smallest time difference
    min_time_diff = filtered_df['time_diff'].min()
    closest_time_entries = filtered_df[filtered_df['time_diff'] == min_time_diff].copy()
    
    if debug:
        print(f"Entries with smallest time diff ({min_time_diff} minutes): {len(closest_time_entries)}")
        print(f"These entries:\n{closest_time_entries}")
    
    # Check if 'Value' and 'Unit' columns exist
    if 'Value' not in closest_time_entries.columns:
        if debug:
            print("No 'Value' column found in weather data")
        return np.nan, ""
    
    # If multiple entries with the same time, take the one with highest Prediction value
    if len(closest_time_entries) > 1:
        if 'Prediction' in closest_time_entries.columns:
            closest_time_entries.loc[:, 'Prediction_Numeric'] = pd.to_numeric(
                closest_time_entries['Prediction'], errors='coerce'
            )
            
            if debug:
                print(f"Multiple entries, Prediction values: {closest_time_entries['Prediction'].tolist()}")
                print(f"Numeric Prediction values: {closest_time_entries['Prediction_Numeric'].tolist()}")
            
            # Sort by Prediction value (descending) and take the first row
            closest_entry = closest_time_entries.sort_values('Prediction_Numeric', ascending=False).iloc[0]
        else:
            closest_entry = closest_time_entries.iloc[0]
    else:
        closest_entry = closest_time_entries.iloc[0]
    
    # Get unit (if available)
    unit = closest_entry.get('Unit', '')
    
    if debug:
        print(f"Selected value: {closest_entry['Value']}, unit: {unit}")
    
    return closest_entry['Value'], unit

def add_weather_data(sensor_csv, output_csv, weather_dir, sites):
    """
    Add weather prediction data to apartment sensor readings
    """
    print(f"Processing {sensor_csv}...")
    
    # Check if the sensor file exists
    if not os.path.exists(sensor_csv):
        print(f"Error: Sensor file {sensor_csv} not found")
        return
    
    # Load sensor data
    sensors_df = pd.read_csv(sensor_csv)
    print(f"Loaded sensor data with {len(sensors_df)} rows and {len(sensors_df.columns)} columns")
    
    # Make sure to add this to keep track of whether any weather data was found
    weather_data_found = False
    
    # Store original DateTime format before conversion
    sensors_df['DateTime_Original'] = sensors_df['DateTime']
    
    # Convert DateTime column to datetime - FIX: Use correct format from the file (YYYY-MM-DD)
    print("Converting DateTime column...")
    try:
        sensors_df['DateTime'] = pd.to_datetime(sensors_df['DateTime'], format='%Y-%m-%d', errors='coerce')
    except Exception as e:
        print(f"Error converting DateTime with YYYY-MM-DD format: {e}")
        try:
            # Try alternative format or auto-detection
            sensors_df['DateTime'] = pd.to_datetime(sensors_df['DateTime'], errors='coerce')
        except Exception as e:
            print(f"Error with alternative DateTime format: {e}")
            # Don't use a dummy date, keep original and exit
            print("Could not convert DateTime column, exiting")
            return
    
    # Check if DateTime conversion was successful
    if sensors_df['DateTime'].isna().all():
        print("ERROR: All DateTime values are NaN after conversion")
        # Restore original DateTime values
        sensors_df['DateTime'] = sensors_df['DateTime_Original']
        sensors_df = sensors_df.drop(columns=['DateTime_Original'])
        # Exit the function
        return
    
    # Get list of all weather prediction files
    weather_files = glob.glob(os.path.join(weather_dir, "Pred_*.csv"))
    print(f"Found {len(weather_files)} weather prediction files")
    
    if len(weather_files) == 0:
        print(f"No weather files found in {weather_dir}")
        # Restore original DateTime values and exit
        sensors_df['DateTime'] = sensors_df['DateTime_Original']
        sensors_df = sensors_df.drop(columns=['DateTime_Original'])
        sensors_df.to_csv(output_csv, index=False)
        print(f"Saved original data to {output_csv} (no weather data)")
        return
    
    # Measurements to include with their mapping
    MEASUREMENT_MAPPING = {
        'PRED_GLOB_ctrl': 'global_radiation',
        'PRED_RELHUM_2M_ctrl': 'humidity',
        'PRED_TOT_PREC_ctrl': 'rain',
        'PRED_T_2M_ctrl': 'temperature'
    }
    
    measurements = list(MEASUREMENT_MAPPING.keys())
    
    # Add columns for each site and measurement with empty values
    for site in sites:
        for measurement in measurements:
            # Use the mapped name instead of the original
            mapped_name = MEASUREMENT_MAPPING[measurement]
            col_name = f"{site}-{mapped_name}"  # We'll add the unit later
            sensors_df[col_name] = np.nan
    
    # Track which weather files we've already loaded
    weather_data_cache = {}
    
    # Process each row
    print("Processing rows...")
    
    for idx, row in sensors_df.iterrows():
        sensor_date = row['DateTime']
        sensor_time = row.get('Hours', '00:00')  # Use get() to handle missing column
        
        # Skip if date is invalid
        if pd.isna(sensor_date):
            continue
        
        # Enable debugging for first few rows
        debug_this_row = idx < 5
        
        if debug_this_row:
            print(f"\n==== DEBUG FOR ROW {idx} ====")
            print(f"DateTime: {sensor_date}, Hours: {sensor_time}")
        
        try:
            # Find the closest weather file for this date
            closest_file = find_closest_weather_file(sensor_date, weather_files)
            
            if debug_this_row:
                print(f"Closest weather file: {closest_file}")
            
            if closest_file is None:
                print(f"No weather file found for date {sensor_date}")
                continue
                
            # Load the weather data (with caching to avoid reloading)
            if closest_file in weather_data_cache:
                weather_df = weather_data_cache[closest_file]
            else:
                if debug_this_row:
                    print(f"Loading weather file: {closest_file}")
                    
                try:
                    weather_df = pd.read_csv(closest_file)
                    
                    # Rename columns if necessary
                    if 'Site' not in weather_df.columns and len(weather_df.columns) >= 6:
                        column_names = ['Time', 'Value', 'Prediction', 'Site', 'Measurement', 'Unit', 'Hour']
                        weather_df.columns = column_names[:len(weather_df.columns)]
                    
                    # Print some details about the file contents
                    if debug_this_row:
                        print(f"Weather file shape: {weather_df.shape}")
                        print(f"Weather file columns: {weather_df.columns.tolist()}")
                except Exception as e:
                    print(f"Error loading weather file {closest_file}: {e}")
                    continue
                
                weather_data_cache[closest_file] = weather_df
            
            # For each site and measurement, find closest prediction
            for site in sites:
                for measurement in measurements:
                    try:
                        value, unit = find_closest_time_prediction(
                            sensor_date, sensor_time, weather_df, site, measurement, 
                            debug=debug_this_row
                        )
                        
                        if not pd.isna(value):
                            # We found data! Set the flag
                            weather_data_found = True
                        
                        # Use the mapped name instead of the original
                        mapped_name = MEASUREMENT_MAPPING[measurement]
                        
                        # Update the column name with unit
                        col_name = f"{site}-{mapped_name}"
                        if unit:  # Add unit to column name if available
                            col_name_with_unit = f"{site}-{mapped_name}-{unit}"
                            # Create column if it doesn't exist
                            if col_name_with_unit not in sensors_df.columns:
                                sensors_df[col_name_with_unit] = np.nan
                            sensors_df.loc[idx, col_name_with_unit] = value
                        else:
                            # Use the original column without unit
                            sensors_df.loc[idx, col_name] = value
                    except Exception as e:
                        if debug_this_row:
                            print(f"Error processing measurement {measurement} for site {site} at row {idx}: {str(e)}")
                    
        except Exception as e:
            if debug_this_row:
                print(f"Error processing row {idx}: {str(e)}")
        
        # Print progress
        if idx % 1000 == 0 and idx > 0:
            print(f"Processed {idx} of {len(sensors_df)} rows...")
    
    # Only remove empty columns if we found weather data
    if weather_data_found:
        # Remove columns that were never filled (all NaN)
        for col in list(sensors_df.columns):
            if col.startswith(tuple(f"{site}-" for site in sites)) and sensors_df[col].isna().all():
                print(f"Removing unused column: {col}")
                sensors_df = sensors_df.drop(columns=[col])
    else:
        print("WARNING: No weather data was found or added to the dataset")
        
    # Restore original DateTime column
    sensors_df['DateTime'] = sensors_df['DateTime_Original']
    sensors_df = sensors_df.drop(columns=['DateTime_Original'])
    
    # Save the result
    sensors_df.to_csv(output_csv, index=False)
    print(f"Saved results to {output_csv}")


sensor_csv_1 = "Apartment_1_latest_2weeks_consumption.csv"
output_csv_1 = "Apartment_1_latest_2weeks_consumption.csv"
add_weather_data(sensor_csv_1, output_csv_1, weather_dir, sites=['Sion'])
sensor_csv_1 = "Apartment_2_latest_2weeks_consumption.csv"
output_csv_1 = "Apartment_2_latest_2weeks_consumption.csv"
add_weather_data(sensor_csv_1, output_csv_1, weather_dir, sites=['Sion'])


# =============================================
# =============== Feature Engineering: Seasons, Lag, Lead, Rolling features
# =============================================


def engineer_apartment_features(file_path):

    # Load the dataset
    df = pd.read_csv(file_path)

    # Convert DateTime to proper datetime format
    df['DateTime'] = pd.to_datetime(df['DateTime'])

    # Create a proper timestamp combining date and hour
    df['Timestamp'] = pd.to_datetime(df['DateTime'].astype(str) + ' ' + df['Hours'])

    # Sort data by timestamp to ensure proper time ordering
    df = df.sort_values('Timestamp')

    # Display information about the dataset
    print(f"Dataset has {len(df)} rows and spans from {df['DateTime'].min()} to {df['DateTime'].max()}")
    print(f"Number of unique dates: {df['DateTime'].nunique()}")

    # Seasonal Features
    # Add a simple season feature based on month
    seasons = {
        12: 'Winter', 1: 'Winter', 2: 'Winter',
        3: 'Spring', 4: 'Spring', 5: 'Spring',
        6: 'Summer', 7: 'Summer', 8: 'Summer',
        9: 'Fall', 10: 'Fall', 11: 'Fall'
    }
    df['Month'] = df['DateTime'].dt.month
    df['Season'] = df['Month'].map(seasons)

    # Create dummy variables for Season (one-hot encoding)
    season_dummies = pd.get_dummies(df['Season'], prefix='Season')

    # Make sure all seasons are represented, even if not in data
    for season in ['Winter', 'Spring', 'Summer', 'Fall']:
        if f'Season_{season}' not in season_dummies.columns:
            season_dummies[f'Season_{season}'] = 0
        else:
            # Ensure all values are stored as integers (0 or 1)
            season_dummies[f'Season_{season}'] = season_dummies[f'Season_{season}'].astype(int)

    # Add season dummies to dataframe
    df = pd.concat([df, season_dummies], axis=1)

    # Remove the Season column
    df = df.drop('Season', axis=1)

    # Lag and Lead Features
    # Sort by timestamp
    df_sorted = df.sort_values('Timestamp')

    # Create lag features (1-hour and 3-hour)
    df_sorted['TotalPower_Sum_lag_1h'] = df_sorted['TotalPower_Sum'].shift(1)
    df_sorted['TotalPower_Sum_lag_3h'] = df_sorted['TotalPower_Sum'].shift(3)

    # Create lead features (1-hour and 3-hour ahead)
    df_sorted['TotalPower_Sum_lead_1h'] = df_sorted['TotalPower_Sum'].shift(-1)
    df_sorted['TotalPower_Sum_lead_3h'] = df_sorted['TotalPower_Sum'].shift(-3)

    # Check for time continuity to handle missing hours
    time_diff_1h = (df_sorted['Timestamp'] - df_sorted['Timestamp'].shift(1)).dt.total_seconds() / 3600
    mask_invalid_1h = (time_diff_1h > 1.5)  # If more than 1.5 hours difference, the lag isn't valid
    df_sorted.loc[mask_invalid_1h, 'TotalPower_Sum_lag_1h'] = np.nan

    # For 3-hour lag, check the time difference
    time_diff_3h = (df_sorted['Timestamp'] - df_sorted['Timestamp'].shift(3)).dt.total_seconds() / 3600
    mask_invalid_3h = (abs(time_diff_3h - 3) > 1.5)  # If not close to 3 hours, the lag isn't valid
    df_sorted.loc[mask_invalid_3h, 'TotalPower_Sum_lag_3h'] = np.nan

    # For 1-hour lead, check the time difference
    lead_time_diff_1h = (df_sorted['Timestamp'].shift(-1) - df_sorted['Timestamp']).dt.total_seconds() / 3600
    mask_invalid_lead_1h = (lead_time_diff_1h > 1.5)  # If more than 1.5 hours difference, the lead isn't valid
    df_sorted.loc[mask_invalid_lead_1h, 'TotalPower_Sum_lead_1h'] = np.nan

    # For 3-hour lead, check the time difference
    lead_time_diff_3h = (df_sorted['Timestamp'].shift(-3) - df_sorted['Timestamp']).dt.total_seconds() / 3600
    mask_invalid_lead_3h = (abs(lead_time_diff_3h - 3) > 1.5)  # If not close to 3 hours, the lead isn't valid
    df_sorted.loc[mask_invalid_lead_3h, 'TotalPower_Sum_lead_3h'] = np.nan

    # Update the original dataframe with the lag and lead features
    df = df_sorted.copy()

    # Rolling Features
    # Create rolling mean features with 3-hour and 6-hour windows
    df['TotalPower_Sum_roll_3h_mean'] = df['TotalPower_Sum'].rolling(window=3, min_periods=1).mean()
    df['TotalPower_Sum_roll_6h_mean'] = df['TotalPower_Sum'].rolling(window=6, min_periods=1).mean()

    # Replace NaN with appropriate values
    # For lag and lead features
    lag_lead_cols = ['TotalPower_Sum_lag_1h', 'TotalPower_Sum_lag_3h', 
                     'TotalPower_Sum_lead_1h', 'TotalPower_Sum_lead_3h']
    for col in lag_lead_cols:
        # Fill with column mean
        df[col] = df[col].fillna(df[col].mean())

    # For rolling features
    roll_cols = ['TotalPower_Sum_roll_3h_mean', 'TotalPower_Sum_roll_6h_mean']
    for col in roll_cols:
        # Fill with column mean
        df[col] = df[col].fillna(df[col].mean())

    # Save the enhanced dataset
    df.to_csv(file_path, index=False)

    return df

engineer_apartment_features('Apartment_1_latest_2weeks_consumption.csv')
engineer_apartment_features('Apartment_2_latest_2weeks_consumption.csv')


# =============================================
# =============== Forecast
# =============================================


def forecast_power_consumption(model_path, data_path, forecast_output_path, forecast_days=3):

    # Predefined feature groups from training
    time_features = ['Hour', 'DayOfWeek', 'IsWeekend', 'Month', 'Year', 'Day', 'DayOfYear']
    seasonal_features = ['Season_Fall', 'Season_Spring', 'Season_Summer', 'Season_Winter']
    weather_features = [
        'Sion-global_radiation-Watt/m2',
        'Sion-humidity-Percent',
        'Sion-rain-Kg/m2',
        'Sion-temperature-°C'
    ]
    lag_features = ['TotalPower_Sum_lag_1h', 'TotalPower_Sum_lag_3h']
    lead_features = ['TotalPower_Sum_lead_1h', 'TotalPower_Sum_lead_3h']
    rolling_features = ['TotalPower_Sum_roll_3h_mean', 'TotalPower_Sum_roll_6h_mean']
    
    # Combine all feature groups in the exact order
    all_features = (
        time_features + 
        seasonal_features + 
        weather_features + 
        lag_features + 
        lead_features + 
        rolling_features
    )
    
    # Load the trained model
    model = joblib.load(model_path)
    
    # Load the preprocessed data
    df = pd.read_csv(data_path)
    
    # Ensure DateTime is datetime
    df['DateTime'] = pd.to_datetime(df['DateTime'])
    
    # Extract additional time components if not already present
    df['Year'] = df['DateTime'].dt.year
    df['Month'] = df['DateTime'].dt.month
    df['Day'] = df['DateTime'].dt.day
    df['DayOfYear'] = df['DateTime'].dt.dayofyear
    
    # Create Timestamp column if it doesn't exist
    if 'Timestamp' not in df.columns:
        # Combine DateTime and Hours
        if 'Hours' in df.columns:
            df['Timestamp'] = pd.to_datetime(df['DateTime'].astype(str) + ' ' + df['Hours'])
        else:
            df['Timestamp'] = df['DateTime']
    
    # Validate and ensure all required features are present
    missing_features = [feat for feat in all_features if feat not in df.columns]
    if missing_features:
        for feat in missing_features:
            if feat.startswith('Season_'):
                # Add dummy columns for missing seasonal features
                df[feat] = 0
            elif feat.startswith('TotalPower_Sum_'):
                # Add columns with mean value for lag/lead/rolling features
                df[feat] = df['TotalPower_Sum'].mean()
    
    # Get the last timestamp in the dataset
    last_timestamp = pd.to_datetime(df['Timestamp'].max())
    
    # Prepare forecast DataFrame
    forecast_features = []
    
    # Generate timestamps for the next 3 days
    forecast_timestamps = pd.date_range(
        start=last_timestamp + timedelta(hours=1), 
        periods=forecast_days * 24, 
        freq='h'
    )
    
    for timestamp in forecast_timestamps:
        # Create a copy of the last row's features
        last_row = df.iloc[-1].copy()
        
        # Update timestamp-related features
        last_row['DateTime'] = timestamp.date()
        last_row['Timestamp'] = timestamp
        last_row['Hours'] = timestamp.strftime('%H:%M')
        last_row['Hour'] = timestamp.hour
        last_row['DayOfWeek'] = timestamp.dayofweek
        last_row['IsWeekend'] = 1 if timestamp.dayofweek >= 5 else 0
        last_row['Year'] = timestamp.year
        last_row['Month'] = timestamp.month
        last_row['Day'] = timestamp.day
        last_row['DayOfYear'] = timestamp.timetuple().tm_yday
        
        # Update seasonal features based on month
        season_mapping = {
            'Season_Winter': 0,
            'Season_Spring': 0,
            'Season_Summer': 0,
            'Season_Fall': 0
        }
        month = timestamp.month
        if month in [12, 1, 2]:
            season_mapping['Season_Winter'] = 1
        elif month in [3, 4, 5]:
            season_mapping['Season_Spring'] = 1
        elif month in [6, 7, 8]:
            season_mapping['Season_Summer'] = 1
        else:
            season_mapping['Season_Fall'] = 1
        
        # Update seasonal features
        for season, value in season_mapping.items():
            last_row[season] = value
        
        # Update lag and lead features with previous predictions or last known values
        if len(forecast_features) > 0:
            last_row['TotalPower_Sum_lag_1h'] = forecast_features[-1]['TotalPower_Sum_lead_1h']
            last_row['TotalPower_Sum_lag_3h'] = forecast_features[-1]['TotalPower_Sum_lead_3h']
        
        # Roll the rolling mean features forward
        if len(forecast_features) >= 3:
            last_row['TotalPower_Sum_roll_3h_mean'] = np.mean([
                forecast_features[-1]['TotalPower_Sum'],
                forecast_features[-2]['TotalPower_Sum'],
                forecast_features[-3]['TotalPower_Sum']
            ])
        if len(forecast_features) >= 6:
            last_row['TotalPower_Sum_roll_6h_mean'] = np.mean([
                forecast_features[-1]['TotalPower_Sum'],
                forecast_features[-2]['TotalPower_Sum'],
                forecast_features[-3]['TotalPower_Sum'],
                forecast_features[-4]['TotalPower_Sum'],
                forecast_features[-5]['TotalPower_Sum'],
                forecast_features[-6]['TotalPower_Sum']
            ])
        
        # Update weather features (using last known values)
        weather_cols = [
            col for col in df.columns 
            if col.startswith('Sion-') and ('Watt/m2' in col or 'Percent' in col or 'Kg/m2' in col or '°C' in col)
        ]
        for col in weather_cols:
            last_row[col] = df[col].iloc[-1]
        
        # Create a DataFrame with features in the exact order the model expects
        features_df = pd.DataFrame(
            [last_row[model.feature_names_in_].values], 
            columns=model.feature_names_in_
        )
        
        # Predict power consumption
        predicted_power = model.predict(features_df)[0]
        
        # Store prediction details
        last_row['TotalPower_Sum'] = predicted_power
        last_row['TotalPower_Sum_lead_1h'] = predicted_power
        last_row['TotalPower_Sum_lead_3h'] = predicted_power
        
        forecast_features.append(last_row)
    
    # Convert forecast features to DataFrame
    forecast_df = pd.DataFrame(forecast_features)
    
    # Get today's date for the forecast generation date
    forecast_date = datetime.now().strftime('%Y-%m-%d')
    forecast_df['Forecast_Date'] = forecast_date
    
    # Select and rename columns for output
    output_columns = [
        'DateTime', 'Hours', 'TotalPower_Sum', 'Forecast_Date'
    ]
    
    # Reorder and select columns
    forecast_df = forecast_df[output_columns]
    
    # Save forecast to CSV
    forecast_df.to_csv(forecast_output_path, index=False)
    
    return forecast_df

# Run the forecast
model_path = './models/random_forest_power_consumption_app1.pkl'
data_path = 'Apartment_1_latest_2weeks_consumption.csv'
forecast_output_path = 'Apartment_1_power_consumption_forecast.csv'
forecast_result = forecast_power_consumption(model_path, data_path, forecast_output_path)

model_path = './models/random_forest_power_consumption_app2.pkl'
data_path = 'Apartment_2_latest_2weeks_consumption.csv'
forecast_output_path = 'Apartment_2_power_consumption_forecast.csv'
forecast_result = forecast_power_consumption(model_path, data_path, forecast_output_path)