import os
import json
import glob
import pandas as pd
from datetime import datetime, timedelta
import pickle
import numpy as np
from sklearn.ensemble import RandomForestRegressor
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from config import Config

config = Config.load()
base_dir = str(config.SENSORS_DATA_DIR)

# =============================================
# =============== Extract and Combine Data
# =============================================


def extract_motion_data(json_data, apartment_id):
    data = {}
    
    # Metadata
    data['DateTime'] = json_data.get('Datetime', '')
    data['Hours'] = json_data.get('Hours', '')
    data['User'] = json_data.get('User', '')
    data['ApartmentID'] = apartment_id
    
    # Extract motion data only
    if 'Motions' in json_data:
        for room, motion_data in json_data['Motions'].items():
            for key, value in motion_data.items():
                data[f'{room}_Motion_{key}'] = value
    
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
                    
                    # Extract motion data
                    motion_data = extract_motion_data(data, apartment_id)
                    if motion_data and any(key for key in motion_data.keys() if 'Motion' in key):
                        all_data.append(motion_data)
                except Exception as e:
                    print(f"Error processing {json_file}: {e}")
    
    return all_data

def extract_latest_weeks_motion_data(base_dir, apartment_id, year, room, num_weeks=2):
    """Extract the most recent N weeks of available motion data for a specific room"""
    print(f"Extracting the last {num_weeks} weeks of available motion data for {apartment_id} - {room}...")
    
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
        print(f"Created initial DataFrame with {len(df)} rows")
        
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
        
        # Add entry counter
        df['EntryCounter'] = 1
        
        # Filter columns for this room
        room_cols = [col for col in df.columns if room in col and 'Motion' in col]
        
        # Convert boolean motion columns to numeric (1/0)
        for col in room_cols:
            # Handle different data types appropriately
            if df[col].dtype == 'bool' or (df[col].dtype == 'object' and set(df[col].dropna().unique()) <= {'True', 'False', True, False}):
                # Try to convert string booleans to numeric
                try:
                    df[col] = df[col].map({'True': 1, 'False': 0, True: 1, False: 0})
                except:
                    pass
        
        # Keep metadata columns and room-specific columns
        keep_cols = ['DateTime', 'Date', 'Hour', 'Hours', 'DayOfWeek', 'IsWeekend', 
                    'EntryCounter', 'ApartmentID', 'User'] + room_cols
        
        # Keep only columns that exist in the DataFrame
        keep_cols = [col for col in keep_cols if col in df.columns]
        
        # Filter the DataFrame
        df = df[keep_cols].copy()
        
        # Sort by date and time
        df = df.sort_values(['DateTime', 'Hour'])
        
        print(f"Final DataFrame has {len(df)} rows with {len(df.columns)} columns")
        return df
    
    print(f"No data found for {apartment_id} - {room}")
    return None

def get_latest_weeks_motion_data(base_dir, apartment_rooms, year, num_weeks=2):
    """Extract the most recent N weeks of available motion data for multiple apartments and rooms"""
    results = {}
    
    for apartment_id, rooms in apartment_rooms.items():
        results[apartment_id] = {}
        
        for room in rooms:
            # Extract data
            df = extract_latest_weeks_motion_data(base_dir, apartment_id, year, room, num_weeks)
            
            if df is not None and not df.empty:
                # Save to CSV
                output_filename = f"{apartment_id}_{room}_latest_{num_weeks}weeks_motion.csv"
                df.to_csv(output_filename, index=False)
                print(f"Saved latest {num_weeks} weeks of motion data for {apartment_id} - {room} to {output_filename}")
                print(f"DataFrame shape: {df.shape}")
                results[apartment_id][room] = df
            else:
                print(f"No motion data found for {apartment_id} - {room}")
                results[apartment_id][room] = None
    
    return results

# Usage
year = "2023"
apartment_rooms = {
    "Apartment_1": ["Kitchen", "Office"],
    "Apartment_2": ["Living", "Office"]
}

# Extract the latest 2 weeks of available data and save to CSV
motion_data = get_latest_weeks_motion_data(base_dir, apartment_rooms, year, num_weeks=2)


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


df_clean = clean_data('Apartment_1_Kitchen_latest_2weeks_motion.csv')
df_clean.to_csv('Apartment_1_Kitchen_latest_2weeks_motion.csv', index=False)

df_clean = clean_data('Apartment_1_Office_latest_2weeks_motion.csv')
df_clean.to_csv('Apartment_1_Office_latest_2weeks_motion.csv', index=False)

df_clean = clean_data('Apartment_2_Living_latest_2weeks_motion.csv')
df_clean.to_csv('Apartment_2_Living_latest_2weeks_motion.csv', index=False)

df_clean = clean_data('Apartment_2_Office_latest_2weeks_motion.csv')
df_clean.to_csv('Apartment_2_Office_latest_2weeks_motion.csv', index=False)


# =============================================
# =============== Process and Remove columns
# =============================================


def process_columns(csv_file, output_file=None):

    # Read the CSV file
    df = pd.read_csv(csv_file)
    
    # Convert DateTime to datetime type
    df['DateTime'] = pd.to_datetime(df['DateTime'])
    
    # Extract minutes from Hours column (format HH:MM)
    df['Minute'] = df['Hours'].str.split(':', expand=True)[1].astype(int)
    
    # Rearrange columns to put Minute right after Hour
    hour_pos = df.columns.get_loc('Hour')
    cols = list(df.columns)
    minute_pos = cols.index('Minute')
    cols.pop(minute_pos)  # Remove Minute from its current position
    cols.insert(hour_pos + 1, 'Minute')  # Insert Minute after Hour
    
    # Drop columns
    cols.remove('Hours')
    cols.remove('EntryCounter')
    cols.remove('Date')
    cols.remove('ApartmentID')
    cols.remove('User')
    
    # Reorder the DataFrame
    df = df[cols]
    
    # If an output file is specified, save the result
    if output_file:
        df.to_csv(output_file, index=False)
        print(f"Processed data saved to {output_file}")
        return None
    
    # Otherwise, return the processed DataFrame
    return df

input_files = [
    'Apartment_1_Kitchen_latest_2weeks_motion.csv',
    'Apartment_1_Office_latest_2weeks_motion.csv', 
    'Apartment_2_Living_latest_2weeks_motion.csv',
    'Apartment_2_Office_latest_2weeks_motion.csv'
]

for input_file in input_files:
    process_columns(input_file, input_file)
    print(f"Processed {input_file}")


# =============================================
# =============== Feature Engineering: Presence features and attribute Presence
# =============================================


def create_presence_features(df, room_name):

    # Create a copy to avoid modifying the original
    room_df = df.copy()
    
    # Ensure DataFrame has a datetime index (if it doesn't already)
    if not isinstance(room_df.index, pd.DatetimeIndex) and 'DateTime' in room_df.columns:
        room_df = room_df.set_index('DateTime')
        room_df.index = pd.to_datetime(room_df.index)
    
    # Extract date only for grouping
    date_only = room_df.index.date if isinstance(room_df.index, pd.DatetimeIndex) else pd.to_datetime(room_df['Date']).dt.date
    
    # Create motion-related features
    motion_col = f"{room_name}_Motion_Motion"
    
    # 2. Create lagged features to capture transitions
    for lag in [1, 3, 5]:
        room_df[f'{room_name}_motion_lag_{lag}'] = room_df.groupby(date_only)[motion_col].shift(lag, fill_value=0)
    
    # 3. Create forward-looking features to capture future actions
    for lead in [1, 3, 5]:
        room_df[f'{room_name}_motion_lead_{lead}'] = room_df.groupby(date_only)[motion_col].shift(-lead, fill_value=0)
    
    return room_df

def label_presence(df, room_name):

    # Create a copy to avoid modifying the original
    labeled_df = df.copy()
    
    # Get the motion column name
    motion_col = f"{room_name}_Motion_Motion"
    
    # Create a new column for presence
    labeled_df[f'{room_name}_Presence'] = 0
    
    # If motion is detected directly, set presence to 1
    labeled_df.loc[labeled_df[motion_col] == 1, f'{room_name}_Presence'] = 1
    
    # Use lag and lead pairs to detect gaps (0s) between motion detections (1s)
    # This ensures we only fill gaps where there is motion both before AND after
    for lag in [1, 3, 5]:
        for lead in [1, 3, 5]:
            lag_col = f'{room_name}_motion_lag_{lag}'
            lead_col = f'{room_name}_motion_lead_{lead}'
            
            # Mark as presence only if there's motion both before AND after
            labeled_df.loc[
                (labeled_df[motion_col] == 0) & 
                (labeled_df[lag_col] == 1) & 
                (labeled_df[lead_col] == 1), 
                f'{room_name}_Presence'
            ] = 1
    
    
    return labeled_df

    
# Load data for both apartments
apt1_kitchen = pd.read_csv('Apartment_1_Kitchen_latest_2weeks_motion.csv')
apt1_office = pd.read_csv('Apartment_1_Office_latest_2weeks_motion.csv')
apt2_living = pd.read_csv('Apartment_2_Living_latest_2weeks_motion.csv')
apt2_office = pd.read_csv('Apartment_2_Office_latest_2weeks_motion.csv')

# Convert DateTime to datetime for proper processing
for df in [apt1_kitchen, apt1_office, apt2_living, apt2_office]:
    df['DateTime'] = pd.to_datetime(df['DateTime'])
    df.set_index('DateTime', inplace=True)
    df.sort_index(inplace=True)

# Process each room with feature engineering
processed_apt1_kitchen = create_presence_features(apt1_kitchen, 'Kitchen')
processed_apt1_office = create_presence_features(apt1_office, 'Office')
processed_apt2_living = create_presence_features(apt2_living, 'Livingroom')
processed_apt2_office = create_presence_features(apt2_office, 'Office')

# Label presence for each room
labeled_apt1_kitchen = label_presence(processed_apt1_kitchen, 'Kitchen')
labeled_apt1_office = label_presence(processed_apt1_office, 'Office')
labeled_apt2_living = label_presence(processed_apt2_living, 'Livingroom')
labeled_apt2_office = label_presence(processed_apt2_office, 'Office')

# Save labeled data
labeled_apt1_kitchen.to_csv('Apartment_1_Kitchen_latest_2weeks_motion.csv')
labeled_apt1_office.to_csv('Apartment_1_Office_latest_2weeks_motion.csv')
labeled_apt2_living.to_csv('Apartment_2_Living_latest_2weeks_motion.csv')
labeled_apt2_office.to_csv('Apartment_2_Office_latest_2weeks_motion.csv')


# =============================================
# =============== Aggregate to hourly data
# =============================================


def aggregate_presence_to_hourly(filepath, room_name):

    # Load the labeled dataset
    df = pd.read_csv(filepath)
    
    # Convert DateTime to proper datetime format if it's not already
    if 'DateTime' in df.columns:
        df['DateTime'] = pd.to_datetime(df['DateTime'])
    else:
        # If DateTime is the index
        df.index = pd.to_datetime(df.index)
        df = df.reset_index()
    
    # Create a Date column for grouping
    df['Date'] = df['DateTime'].dt.date
    
    # Drop lag and lead columns as they're no longer needed
    lag_lead_cols = [col for col in df.columns if 'lag' in col or 'lead' in col]
    df = df.drop(columns=lag_lead_cols)
    
    # Define columns
    motion_col = f"{room_name}_Motion_Motion"
    presence_col = f"{room_name}_Presence"
    temp_col = f"{room_name}_Motion_Temperature"
    light_col = f"{room_name}_Motion_Light"
    
    # Prepare aggregation dictionary - correct format
    agg_dict = {
        'DayOfWeek': 'first',
        'IsWeekend': 'first',
        motion_col: 'sum',  # Sum of motion events
        presence_col: 'sum'  # Sum of presence minutes
    }
    
    # Add temperature and light columns if they exist
    if temp_col in df.columns:
        agg_dict[temp_col] = 'mean'
    if light_col in df.columns:
        agg_dict[light_col] = 'mean'
    
    # Add entry count using a different approach
    df['EntryCount'] = 1  # Each row is one entry
    agg_dict['EntryCount'] = 'sum'
    
    # Group by date and hour
    hourly_data = df.groupby(['Date', 'Hour']).agg(agg_dict).reset_index()
    
    # Rename columns to reflect the aggregation method
    column_renames = {
        motion_col: f"{motion_col}_Sum",
        presence_col: f"{presence_col}_Sum"
    }
    
    if temp_col in hourly_data.columns:
        column_renames[temp_col] = f"{temp_col}_Mean"
    if light_col in hourly_data.columns:
        column_renames[light_col] = f"{light_col}_Mean"
    
    hourly_data = hourly_data.rename(columns=column_renames)
    
    # Format Hours as HH:00
    hourly_data['Hours'] = hourly_data['Hour'].apply(lambda x: f"{x:02d}:00")
    
    # Rename Date column to DateTime to maintain format from original script
    hourly_data = hourly_data.rename(columns={'Date': 'DateTime'})
    
    # Calculate presence percentage for the hour
    hourly_data[f'{room_name}_Presence_Percentage'] = (hourly_data[f"{presence_col}_Sum"] / hourly_data['EntryCount'] * 100).round(2)
    
    # Reorder columns to have DateTime and Hours at the beginning
    base_columns = ['DateTime', 'Hours', 'Hour', 'DayOfWeek', 'IsWeekend']
    data_columns = [col for col in hourly_data.columns if col not in base_columns + ['EntryCount']]
    columns_order = base_columns + data_columns + ['EntryCount']
    hourly_data = hourly_data[columns_order]
    
    # Check for hours with fewer than expected entries
    expected_entries = 60  # 60 minutes per hour
    missing_data = hourly_data[hourly_data['EntryCount'] < expected_entries]
    print(f"\nHours with fewer than {expected_entries} entries (potentially missing data):")
    print(missing_data[['DateTime', 'Hours', 'EntryCount']])
    
    # Calculate average entries per hour
    avg_entries = hourly_data['EntryCount'].mean()
    print(f"\nAverage entries per hour: {avg_entries:.2f}")
    
    return hourly_data

# Process each room's data
rooms = {
    'Apartment_1_Kitchen_latest_2weeks_motion.csv': 'Kitchen',
    'Apartment_1_Office_latest_2weeks_motion.csv': 'Office',
    'Apartment_2_Living_latest_2weeks_motion.csv': 'Livingroom',
    'Apartment_2_Office_latest_2weeks_motion.csv': 'Office'
}

for filepath, room_name in rooms.items():
    print(f"Processing {room_name} data from {filepath}")
    hourly_data = aggregate_presence_to_hourly(filepath, room_name)
    
    # Save the hourly aggregated data
    output_filepath = f"{filepath}"
    hourly_data.to_csv(output_filepath, index=False)
    print(f"Hourly aggregated data saved to '{output_filepath}'")
    print(f"First few rows of {room_name} hourly data:")
    print(hourly_data.head(5))
    print("-" * 80)


# =============================================
# =============== Forecast
# =============================================


def load_model(model_path):
    try:
        with open(model_path, 'rb') as f:
            model = pickle.load(f)
        print(f"Successfully loaded model from {model_path}")
        return model
    except Exception as e:
        print(f"Error loading model from {model_path}: {e}")
        return None

def prepare_forecast_features(file_path, room_name, apartment_number):

    # Load the dataset
    df = pd.read_csv(file_path)
    
    # Convert DateTime to proper datetime format
    df['DateTime'] = pd.to_datetime(df['DateTime'])
    
    # Extract time components
    df['Year'] = df['DateTime'].dt.year
    df['Month'] = df['DateTime'].dt.month
    df['Day'] = df['DateTime'].dt.day
    df['DayOfYear'] = df['DateTime'].dt.dayofyear
    
    # Define the target variable based on room name
    target = f'{room_name}_Presence_Sum'
    
    # Define feature columns (same as training)
    features = [
        'Hour', 'DayOfWeek', 'IsWeekend', 'Month', 'Year', 'Day', 'DayOfYear',
        f'{room_name}_Motion_Motion_Sum'
    ]
    
    # Add optional features if they exist
    optional_features = [
        f'{room_name}_Motion_Temperature_Mean',
        f'{room_name}_Motion_Light_Mean'
    ]
    
    for feature in optional_features:
        if feature in df.columns:
            features.append(feature)
    
    # Filter to only include columns that exist in the DataFrame
    features = [col for col in features if col in df.columns]
    
    print(f"Prepared forecast features for Apartment {apartment_number} {room_name}:")
    print(f"Target variable: {target}")
    print(f"Features: {features}")
    
    return df, target, features

def generate_future_dates(latest_date, num_days=3):
    if isinstance(latest_date, str):
        latest_date = pd.to_datetime(latest_date)
    
    start_date = latest_date + timedelta(days=1)
    end_date = start_date + timedelta(days=num_days-1)
    
    dates = []
    current_date = start_date
    while current_date <= end_date:
        for hour in range(24):
            dates.append((current_date.strftime('%Y-%m-%d'), hour))
        current_date += timedelta(days=1)
    
    return dates

def create_future_features(df, future_dates, room_name, features):
    # Get latest date from dataframe
    latest_date = pd.to_datetime(df['DateTime']).max()
    print(f"Latest date in data: {latest_date}")
    print(f"Generating forecast from {latest_date + timedelta(days=1)} for 3 days")
    
    future_rows = []
    for date_str, hour in future_dates:
        date_obj = pd.to_datetime(date_str)
        
        # Create a row with basic features
        row = {
            'DateTime': date_obj,
            'Hour': hour,
            'Hours': f"{hour:02d}:00",
            'DayOfWeek': date_obj.dayofweek,
            'IsWeekend': 1 if date_obj.dayofweek >= 5 else 0,
            'Year': date_obj.year,
            'Month': date_obj.month,
            'Day': date_obj.day,
            'DayOfYear': date_obj.dayofyear
        }
        
        # For room-specific motion stats, use averages from same hour/day of week
        same_hour_day = df[(df['Hour'] == hour) & (df['DayOfWeek'] == date_obj.dayofweek)]
        
        # Find room-specific columns to average (based on features list)
        for col in df.columns:
            if room_name in col and ('Motion' in col or 'Temperature' in col or 'Light' in col):
                if col in features:
                    if not same_hour_day.empty:
                        row[col] = same_hour_day[col].mean()
                    else:
                        # Fallback to same hour any day
                        same_hour = df[df['Hour'] == hour]
                        if not same_hour.empty:
                            row[col] = same_hour[col].mean()
                        else:
                            row[col] = df[col].mean()  # Global average
        
        future_rows.append(row)
    
    future_df = pd.DataFrame(future_rows)
    return future_df

def make_predictions(model, future_features, feature_columns):
    # Ensure future_features has all required columns
    missing_cols = [col for col in feature_columns if col not in future_features.columns]
    if missing_cols:
        print(f"Warning: Missing columns in future features: {missing_cols}")
        # For missing columns, add them with zeros
        for col in missing_cols:
            future_features[col] = 0
    
    # Select only the required features in the right order
    X_future = future_features[feature_columns]
    
    # Make predictions
    predictions = model.predict(X_future)
    
    # Add predictions to future_features
    future_features['Predicted_Presence'] = predictions
    
    # Ensure predictions are non-negative
    future_features['Predicted_Presence'] = future_features['Predicted_Presence'].clip(lower=0)
    
    # Get today's date for the forecast generation date
    forecast_date = datetime.now().strftime('%Y-%m-%d')
    
    # Filter to only keep the requested columns
    simplified_output = future_features[['DateTime', 'Hours', 'Predicted_Presence']].copy()
    simplified_output['Forecast_Date'] = forecast_date
    
    return simplified_output

# Define configurations for each room
rooms = [
    {
        'apartment': 1,
        'room': 'Kitchen',
        'model_path': './models/rf_apartment1_kitchen.pkl',
        'data_path': 'Apartment_1_Kitchen_latest_2weeks_motion.csv',
    },
    {
        'apartment': 1,
        'room': 'Office',
        'model_path': './models/rf_apartment1_office.pkl',
        'data_path': 'Apartment_1_Office_latest_2weeks_motion.csv',
    },
    {
        'apartment': 2,
        'room': 'Livingroom',
        'model_path': './models/rf_apartment2_living.pkl',
        'data_path': 'Apartment_2_Living_latest_2weeks_motion.csv',
    },
    {
        'apartment': 2,
        'room': 'Office',
        'model_path': './models/rf_apartment2_office.pkl',
        'data_path': 'Apartment_2_Office_latest_2weeks_motion.csv',
    }
]

# Process each room
for room_config in rooms:
    apartment_num = room_config['apartment']
    room_name = room_config['room']
    model_path = room_config['model_path']
    data_path = room_config['data_path']
    
    print(f"\n{'='*60}")
    print(f"Processing Apartment {apartment_num} - {room_name}")
    print(f"{'='*60}")
    
    # Load model
    model = load_model(model_path)
    if model is None:
        print(f"Skipping Apartment {apartment_num} - {room_name} due to model loading error")
        continue
    
    # Prepare features (using the same approach as in training)
    df, target, features = prepare_forecast_features(data_path, room_name, apartment_num)
    
    # Generate future dates (next 3 days)
    latest_date = pd.to_datetime(df['DateTime']).max()
    future_dates = generate_future_dates(latest_date, num_days=3)
    print(f"Generated {len(future_dates)} future time points for forecasting")
    
    # Create future features
    future_features = create_future_features(df, future_dates, room_name, features)
    
    # Make predictions
    forecast_data = make_predictions(model, future_features, features)
    
    # Save forecast to CSV
    output_filename = f"Apartment_{apartment_num}_{room_name}_motion_forecast.csv"
    forecast_data.to_csv(output_filename, index=False)
    print(f"Forecast saved to {output_filename}")


