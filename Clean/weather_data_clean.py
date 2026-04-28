import os
import glob
import pandas as pd
import logging
import threading
from datetime import datetime
from pathlib import Path
import concurrent.futures
from typing import List, Optional, Tuple, Dict, Any
from config import Config, ensure_directory


def setup_logging() -> logging.Logger:
    """Set up logging configuration."""
    # Load configuration
    config = Config.load()
    
    # Create log directory
    ensure_directory(config.WEATHER_LOG_DIR)
    
    # Create log file with timestamp
    log_file = config.WEATHER_LOG_DIR / f'weather_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.log'
    
    # Configure logging with UTF-8 encoding to handle non-ASCII characters
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] - [%(levelname)s] - [%(name)s] - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger('weather')


def validate_dataframe(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """Validate the dataframe structure and content."""
    issues = []
    
    # Check required columns
    required_columns = ['Time', 'Value', 'Prediction', 'Site', 'Measurement', 'Unit']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        issues.append(f"Missing required columns: {', '.join(missing_columns)}")
        return False, issues
    
    # Check for null values in critical columns
    for col in ['Time', 'Site', 'Measurement']:
        if df[col].isnull().any():
            issues.append(f"Null values found in critical column: {col}")
    
    # Note: We remove the site validation check since we want to filter for sites later
    # rather than reject files with additional sites
    
    return len(issues) == 0, issues


def process_file(file_path: str, output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
    """Process a single CSV file and return metrics."""
    thread_id = threading.get_ident()
    start_time = datetime.now()
    file_name = os.path.basename(file_path)
    output_path = os.path.join(output_dir, file_name)
    
    # Check if file has already been processed
    if os.path.exists(output_path):
        logger.info(f"[Thread-{thread_id}] SKIPPED: {file_name} - already processed")
        return {
            "file_name": file_name,
            "status": "skipped",
            "rows_before": 0,
            "rows_after": 0,
            "duration_seconds": 0,
            "thread_id": thread_id
        }
    
    metrics = {
        "file_name": file_name,
        "start_time": start_time,
        "rows_before": 0,
        "rows_after": 0,
        "status": "failed",
        "errors": [],
        "duration_seconds": 0,  # Initialize with a default value
        "thread_id": thread_id
    }
    
    try:
        # Read the CSV file
        df = pd.read_csv(file_path)
        metrics["rows_before"] = len(df)
        
        # Validate dataframe for required columns
        is_valid, issues = validate_dataframe(df)
        if not is_valid:
            metrics["errors"].extend(issues)
            # Use sanitized logging to avoid encoding issues
            logger.error(f"[Thread-{thread_id}] VALIDATION FAILED: {file_name} - {issues[0]}")
            return metrics
        
        # Filter for specific sites - do this regardless of validation
        original_rows = len(df)
        df = df[df['Site'].isin(['Sion', 'Visp'])]
        logger.info(f"[Thread-{thread_id}] FILTERED: {file_name} - {original_rows} → {len(df)} rows for Sion/Visp sites")
        
        # Replace -99999 with 0
        df = df.replace(-99999, 0)
        
        # Process timestamp
        if 'Time' in df.columns:
            # Check if Time already contains a space (datetime format)
            if df['Time'].astype(str).str.contains(' ').any():
                # Split datetime into date and time
                df['Time'] = df['Time'].astype(str)
                splitted = df['Time'].str.split(' ', n=1, expand=True)
                
                # Convert date and create Hour column
                try:
                    date_part = pd.to_datetime(splitted[0], errors='coerce')
                    df['Time'] = date_part.dt.strftime('%d-%m-%Y')
                    df['Hour'] = splitted[1]
                except Exception as e:
                    metrics["errors"].append(f"Date conversion error: {str(e)}")
                    logger.error(f"[Thread-{thread_id}] DATE ERROR: {file_name} - {str(e)}")
            else:
                # If Time doesn't contain a space, assume it's already in the right format
                logger.info(f"[Thread-{thread_id}] FORMAT NOTE: Time column in {file_name} doesn't need splitting")
        
        # Skip empty dataframes after filtering
        if len(df) == 0:
            metrics["errors"].append("No rows matching filter criteria (Sion or Visp)")
            logger.warning(f"[Thread-{thread_id}] EMPTY RESULT: {file_name} - No matching rows after filtering")
            metrics["status"] = "warning"
            metrics["rows_after"] = 0
            metrics["end_time"] = datetime.now()
            metrics["duration_seconds"] = (metrics["end_time"] - start_time).total_seconds()
            return metrics
        
        # Save the cleaned dataframe - without adding the metadata columns
        df.to_csv(output_path, index=False, encoding='utf-8')
        
        metrics["rows_after"] = len(df)
        metrics["status"] = "success"
        metrics["end_time"] = datetime.now()
        metrics["duration_seconds"] = (metrics["end_time"] - start_time).total_seconds()
        
        logger.info(f"[Thread-{thread_id}] SUCCESS: {file_name} - {metrics['rows_before']} → {metrics['rows_after']} rows")
        return metrics
        
    except Exception as e:
        metrics["errors"].append(str(e))
        metrics["end_time"] = datetime.now()
        metrics["duration_seconds"] = (metrics["end_time"] - start_time).total_seconds()
        logger.error(f"[Thread-{thread_id}] PROCESS ERROR: {file_name} - {str(e)}")
        return metrics


def clean_weather_data() -> None:
    """
    Clean weather data from CSV files using configuration from config module.
    """
    
    # Load configuration
    config = Config.load()
    
    # Setup logging
    logger = setup_logging()
    
    logger.info(f"STARTING: Weather ETL process")
    logger.info(f"CONFIG: Input directory = {config.RAW_WEATHER_ROOT}")
    logger.info(f"CONFIG: Output directory = {config.CLEAN_WEATHER_ROOT}")
    logger.info(f"CONFIG: Maximum workers = {config.WEATHER_MAX_WORKERS}")

    
    # Create output directory
    os.makedirs(config.CLEAN_WEATHER_ROOT, exist_ok=True)
    
    # Get list of CSV files
    csv_files = glob.glob(os.path.join(config.RAW_WEATHER_ROOT, "*.csv"))
    total_files = len(csv_files)
    logger.info(f"FOUND: {total_files} CSV files to process")
    
    if total_files == 0:
        logger.warning(f"NO FILES: No CSV files found in {config.RAW_WEATHER_ROOT}")
        return
    
    # Process files in parallel
    metrics_list = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=config.WEATHER_MAX_WORKERS) as executor:
        future_to_file = {
            executor.submit(process_file, csv_file, str(config.CLEAN_WEATHER_ROOT), logger): csv_file 
            for csv_file in csv_files
        }
        
        for i, future in enumerate(concurrent.futures.as_completed(future_to_file)):
            metrics = future.result()
            metrics_list.append(metrics)
            logger.info(f"PROGRESS: {i+1}/{total_files} files completed")
    
    # Create summary metrics - handle empty metrics list
    if not metrics_list:
        logger.error("PROCESS FAILED: No files were processed successfully")
        return
        
    # Calculate summary statistics
    skipped_count = sum(1 for m in metrics_list if m['status'] == 'skipped')
    success_count = sum(1 for m in metrics_list if m['status'] == 'success')
    failure_count = sum(1 for m in metrics_list if m['status'] == 'failed')
    warning_count = sum(1 for m in metrics_list if m['status'] == 'warning')
    
    total_rows_before = sum(m['rows_before'] for m in metrics_list)
    total_rows_after = sum(m['rows_after'] for m in metrics_list)
    
    # Calculate percentage safely
    percentage = 0 if total_rows_before == 0 else (total_rows_after/total_rows_before*100)
    
    # Calculate average duration for files that were actually processed
    processed_durations = [m['duration_seconds'] for m in metrics_list if m['status'] != 'skipped' and m['duration_seconds'] > 0]
    avg_duration = sum(processed_durations) / len(processed_durations) if processed_durations else 0
    
    # Log detailed metrics for each file
    logger.info("Individual file processing results:")
    for m in metrics_list:
        if m['status'] == 'skipped':
            logger.info(f"  - {m['file_name']} [Thread-{m['thread_id']}]: Skipped (already processed)")
        elif m['status'] == 'success':
            logger.info(f"  - {m['file_name']} [Thread-{m['thread_id']}]: Success ({m['rows_before']} → {m['rows_after']} rows, {m['duration_seconds']:.2f}s)")
        elif m['status'] == 'warning':
            logger.info(f"  - {m['file_name']} [Thread-{m['thread_id']}]: Warning ({m.get('errors', ['No matching data'])[0]})")
        else:  # failed
            logger.info(f"  - {m['file_name']} [Thread-{m['thread_id']}]: Failed ({m.get('errors', ['Unknown error'])[0]})")
    
    # Log summary metrics with improved formatting
    logger.info("+" + "="*48 + "+")
    logger.info("|" + " WEATHER ETL PROCESS SUMMARY ".center(48) + "|")
    logger.info("+" + "="*48 + "+")
    logger.info(f"| Total files found:           {total_files:4d} |")
    logger.info(f"| Files skipped:               {skipped_count:4d} |")
    logger.info(f"| Files processed successfully: {success_count:4d} |")
    logger.info(f"| Files with warnings:         {warning_count:4d} |")
    logger.info(f"| Files failed:                {failure_count:4d} |")
    logger.info(f"| Total rows before:           {total_rows_before:4d} |")
    logger.info(f"| Total rows after:            {total_rows_after:4d} |")
    logger.info(f"| Percentage retained:        {percentage:6.2f}% |")
    logger.info(f"| Average processing time:    {avg_duration:6.2f}s |")
    logger.info("+" + "="*48 + "+")
    
    # Count number of unique threads used
    thread_ids = set(m.get('thread_id', 0) for m in metrics_list)
    logger.info(f"Number of threads used: {len(thread_ids)}")
    
    logger.info("COMPLETED: Weather ETL process")


def main():
    clean_weather_data()
    
    
if __name__ == "__main__":
    main()