import os
import json
import re
import logging
import concurrent.futures
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Any, Union, Optional
from config import Config, ensure_directory


def setup_logging() -> logging.Logger:
    """Set up logging configuration."""
    # Load configuration
    config = Config.load()
    
    # Create log directory
    ensure_directory(config.SENSOR_LOG_DIR)
    
    # Create log file with timestamp
    log_file = config.SENSOR_LOG_DIR / f'sensor_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.log'
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] - [%(levelname)s] - [%(name)s] - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger('sensor')


def to_pascal_case(s: str) -> str:
    parts = re.split(r'[_\s]', s)
    return ''.join(part.capitalize() for part in parts if part)


def convert_keys(obj: Any) -> Any:
    """
    Recursively traverse the JSON object and convert keys to PascalCase.
    If the converted key is "Switch", transform its value to boolean.
    """
    if isinstance(obj, dict):
        new_obj = {}
        for key, value in obj.items():
            new_key = to_pascal_case(key)
            # Transform "Switch" field to boolean
            if new_key == "Switch":
                if isinstance(value, str):
                    if value.lower() == "on":
                        value = True
                    elif value.lower() == "off":
                        value = False
            new_obj[new_key] = convert_keys(value)
        return new_obj
    elif isinstance(obj, list):
        return [convert_keys(item) for item in obj]
    else:
        return obj


def process_dates(obj: Any, logger: logging.Logger) -> Any:
    """
    Recursively traverse the JSON object to standardize dates and extract time.
    For keys "Datetime" (format "dd.MM.yyyy HH:mm") and for keys
    "TimePlug" or "TimeConsumption" (Unix timestamps), the date is reformatted
    to "dd/mm/yyyy" and the extracted time is placed in a new key "Hours" in format "HH:MM".
    """
    if isinstance(obj, dict):
        for key, value in list(obj.items()):
            if key in ("Datetime", "TimePlug", "TimeConsumption"):
                try:
                    if key == "Datetime" and isinstance(value, str):
                        # Handle various formats, but keep hours and minutes only
                        if ":" in value:
                            # If format includes time
                            dt = datetime.strptime(value, "%d.%m.%Y %H:%M" if value.count(":") == 1 else "%d.%m.%Y %H:%M:%S")
                        else:
                            # If no time in format
                            dt = datetime.strptime(value, "%d.%m.%Y")
                    elif key in ("TimePlug", "TimeConsumption") and isinstance(value, (int, float)):
                        dt = datetime.fromtimestamp(value)
                    else:
                        continue
                    formatted_date = dt.strftime("%d/%m/%Y")
                    hours = dt.strftime("%H:%M")  # Use HH:MM format without seconds
                    obj[key] = formatted_date
                    if "Hours" not in obj:
                        obj["Hours"] = hours
                except Exception as e:
                    logger.error(f"[Thread-{threading.get_ident()}] Date processing error: Key={key}, Value={value}, Error={e}")
            else:
                process_dates(value, logger)
        return obj
    elif isinstance(obj, list):
        for item in obj:
            process_dates(item, logger)
        return obj
    else:
        return obj


def is_valid_room(room_name: str) -> bool:
    """
    Check if a room name is in the list of valid rooms.
    """
    valid_rooms = ["Bdroom", "Bhroom", "Office", "Livingroom", "Kitchen", "Laundry", "Outdoor", "Outside", "House"]
    return room_name in valid_rooms


def remove_device_entries(data: Dict, logger: logging.Logger, filepath: Path, destination_base: Path, apartment: str) -> Dict:
    """
    Remove device entries from the JSON data and log them to a text file.
    """
    thread_id = threading.get_ident()
    removed_devices = {}
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    # Create a copy of data to avoid modifying while iterating
    data_copy = data.copy()
    
    # Process each sensor category
    for sensor_category, sensor_data in data_copy.items():
        if isinstance(sensor_data, dict):
            # Process only Plugs, Doorswindows, Motions, Meteos, Humidities
            if sensor_category in ["Plugs", "Doorswindows", "Motions", "Meteos", "Humidities"]:
                # Special case for Meteos which has a nested "Meteo" key
                if sensor_category == "Meteos" and "Meteo" in sensor_data:
                    meteo_data = sensor_data["Meteo"].copy()
                    for room_name in list(meteo_data.keys()):
                        if not is_valid_room(room_name):
                            # Save removed device data
                            if "RemovedDevices" not in removed_devices:
                                removed_devices["RemovedDevices"] = {}
                            removed_devices["RemovedDevices"][f"{sensor_category}_{room_name}"] = meteo_data[room_name]
                            # Remove device from data
                            del data["Meteos"]["Meteo"][room_name]
                            logger.info(f"[Thread-{thread_id}] Removed device entry: {room_name} from {sensor_category}")
                else:
                    # Process other sensor categories
                    for room_name in list(sensor_data.keys()):
                        if not is_valid_room(room_name):
                            # Save removed device data
                            if "RemovedDevices" not in removed_devices:
                                removed_devices["RemovedDevices"] = {}
                            removed_devices["RemovedDevices"][f"{sensor_category}_{room_name}"] = sensor_data[room_name]
                            # Remove device from data
                            del data[sensor_category][room_name]
                            logger.info(f"[Thread-{thread_id}] Removed device entry: {room_name} from {sensor_category}")
    
    # If there are removed devices, log them to a file
    if removed_devices and "RemovedDevices" in removed_devices:
        # Get datetime and user from the data for the log
        datetime_str = data.get("Datetime", current_date)
        hours_str = data.get("Hours", datetime.now().strftime("%H:%M"))
        user_str = data.get("User", "Unknown")
        
        # Prepare log entry
        log_entry = {
            "Datetime": datetime_str,
            "Hours": hours_str,
            "User": user_str,
            "RemovedDevices": removed_devices["RemovedDevices"]
        }
        
        # Save to the same directory as the script
        script_dir = Path(__file__).parent
        devices_log_file = script_dir / f"removed_devices_{current_date}.txt"
        
        # Append to the log file
        try:
            with open(devices_log_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(log_entry, indent=2, ensure_ascii=False))
                f.write("\n\n")  # Add separator between entries
            logger.info(f"[Thread-{thread_id}] Logged {len(removed_devices['RemovedDevices'])} removed devices to {devices_log_file}")
        except Exception as e:
            logger.error(f"[Thread-{thread_id}] Error writing to devices log file: {e}")
    
    return data


def clean_json(data: Dict, logger: logging.Logger, filepath: Path, destination_base: Path, apartment: str) -> Dict:
    """
    Apply transformations to the JSON:
    - Convert keys to PascalCase and transform "Switch"
    - Standardize dates and extract time
    - Remove device entries and log them
    """
    logger.debug(f"[Thread-{threading.get_ident()}] Starting JSON cleaning process")
    data_converted = convert_keys(data)
    data_processed = process_dates(data_converted, logger)
    data_cleaned = remove_device_entries(data_processed, logger, filepath, destination_base, apartment)
    return data_cleaned


def process_file(filepath: Path, destination_base: Path, logger: logging.Logger) -> Dict[str, Any]:
    """
    Process a single JSON file:
    - Extract the date from the filename (expected format: "dd.MM.yyyy HHMM_name_received.json")
    - Determine the apartment based on the name in the file ("JimmyLoup" or "JeremieVianin")
    - Build the destination path according to the structure Destination\Apartment_X\YYYY\MM\DD
    - Check if the file has already been processed based on destination file existence.
    
    Returns a dictionary with processing status and metrics.
    """
    thread_id = threading.get_ident()
    start_time = datetime.now()
    filename = filepath.name
    
    # Prepare metrics dict
    metrics = {
        "file_name": filename,
        "start_time": start_time,
        "status": "failed",
        "error_msg": None,
        "duration_seconds": 0,
        "thread_id": thread_id
    }
    
    # Determine the apartment
    if "JimmyLoup" in filename:
        apartment = "Apartment_1"
    elif "JeremieVianin" in filename:
        apartment = "Apartment_2"
    else:
        error_msg = f"Unknown apartment name in file {filename}"
        logger.warning(f"[Thread-{thread_id}] FILE IGNORED: {error_msg}")
        metrics["error_msg"] = error_msg
        metrics["end_time"] = datetime.now()
        metrics["duration_seconds"] = (metrics["end_time"] - start_time).total_seconds()
        return metrics

    # Critical fix: Make sure the apartment name is preserved exactly as in the source
    source_dir = str(filepath.parent)
    if "Apartment_1" in source_dir:
        apartment = "Apartment_1"
    elif "Apartment_2" in source_dir:
        apartment = "Apartment_2"
    
    name_no_ext = filepath.stem
    parts = name_no_ext.split(" ")
    if len(parts) < 2:
        error_msg = f"Filename {filename} does not match the expected format"
        logger.warning(f"[Thread-{thread_id}] FORMAT ERROR: {error_msg}")
        metrics["error_msg"] = error_msg
        metrics["end_time"] = datetime.now()
        metrics["duration_seconds"] = (metrics["end_time"] - start_time).total_seconds()
        return metrics

    # Extract date from filename (first part)
    date_str = parts[0]  # Example: "01.06.2023"
    try:
        dt = datetime.strptime(date_str, "%d.%m.%Y")
    except Exception as e:
        error_msg = f"Error extracting date from file {filename}: {e}"
        logger.error(f"[Thread-{thread_id}] DATE ERROR: {error_msg}")
        metrics["error_msg"] = error_msg
        metrics["end_time"] = datetime.now()
        metrics["duration_seconds"] = (metrics["end_time"] - start_time).total_seconds()
        return metrics

    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")

    # Build destination path with desired structure
    dest_dir = destination_base / apartment / year / month / day
    os.makedirs(dest_dir, exist_ok=True)
    dest_path = dest_dir / filename
    
    # Skip if destination file already exists
    if dest_path.exists():
        logger.info(f"[Thread-{thread_id}] SKIPPED: File {filename} already processed")
        metrics["status"] = "skipped"
        metrics["end_time"] = datetime.now()
        metrics["duration_seconds"] = (metrics["end_time"] - start_time).total_seconds()
        return metrics

    # Read source file
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        error_msg = f"Error reading file {filepath}: {e}"
        logger.error(f"[Thread-{thread_id}] READ ERROR: {error_msg}")
        metrics["error_msg"] = error_msg
        metrics["end_time"] = datetime.now()
        metrics["duration_seconds"] = (metrics["end_time"] - start_time).total_seconds()
        return metrics

    # Clean JSON
    try:
        cleaned_data = clean_json(data, logger, filepath, destination_base, apartment)
    except Exception as e:
        error_msg = f"Error cleaning JSON data in {filepath}: {e}"
        logger.error(f"[Thread-{thread_id}] CLEAN ERROR: {error_msg}")
        metrics["error_msg"] = error_msg
        metrics["end_time"] = datetime.now()
        metrics["duration_seconds"] = (metrics["end_time"] - start_time).total_seconds()
        return metrics

    # Save the cleaned file to the destination folder
    try:
        with open(dest_path, "w", encoding="utf-8") as f:
            json.dump(cleaned_data, f, indent=4, ensure_ascii=False)
        logger.info(f"[Thread-{thread_id}] SUCCESS: Processed {filename} -> {dest_path}")
        metrics["status"] = "success"
    except Exception as e:
        error_msg = f"Error saving cleaned file to {dest_path}: {e}"
        logger.error(f"[Thread-{thread_id}] WRITE ERROR: {error_msg}")
        metrics["error_msg"] = error_msg
        metrics["status"] = "failed"
    
    # Calculate and return metrics
    metrics["end_time"] = datetime.now()
    metrics["duration_seconds"] = (metrics["end_time"] - start_time).total_seconds()
    return metrics


def find_json_files(source_dir: Path) -> List[Path]:
    """Find all JSON files in the source directory and its subdirectories."""
    return list(source_dir.glob("**/*.json"))


def clean_sensor_data() -> None:
    """
    Main function to process sensor data.
    """
    
    # Load configuration
    config = Config.load()
    
    # Setup logging
    logger = setup_logging()
    
    logger.info("STARTING: Sensor Data ETL process")
    logger.info(f"CONFIG: Source directories: {config.RAW_SENSOR_ROOT}")
    logger.info(f"CONFIG: Destination directory: {config.CLEAN_SENSOR_ROOT}")
    logger.info(f"CONFIG: Maximum workers: {config.SENSOR_MAX_WORKERS}")
    
    # Source directories - using paths from config
    apartment1_dir = config.RAW_SENSOR_ROOT / "Apartment_1"
    apartment2_dir = config.RAW_SENSOR_ROOT / "Apartment_2"
    
    source_dirs = [apartment1_dir, apartment2_dir]
    
    # Destination directory for cleaned files
    destination_base = config.CLEAN_SENSOR_ROOT
    os.makedirs(destination_base, exist_ok=True)
    
    # Find all JSON files in source directories
    all_files = []
    for source_dir in source_dirs:
        if not source_dir.exists():
            logger.warning(f"SOURCE NOT FOUND: Directory does not exist: {source_dir}")
            continue
        
        logger.info(f"SCANNING: Finding JSON files in {source_dir}")
        dir_files = find_json_files(source_dir)
        logger.info(f"FOUND: {len(dir_files)} files in {source_dir}")
        all_files.extend(dir_files)
    
    total_files = len(all_files)
    logger.info(f"TOTAL: Found {total_files} JSON files to process")
    
    if total_files == 0:
        logger.warning("NO FILES: No JSON files found in any source directory")
        return
    
    # Process files in parallel
    metrics_list = []
    completed = 0
    failed = 0
    skipped = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=config.SENSOR_MAX_WORKERS) as executor:
        # Submit all tasks
        future_to_file = {
            executor.submit(process_file, file_path, destination_base, logger): file_path
            for file_path in all_files
        }
        
        # Process results as they complete
        for i, future in enumerate(concurrent.futures.as_completed(future_to_file)):
            try:
                metrics = future.result()
                metrics_list.append(metrics)
                
                if metrics["status"] == "success":
                    completed += 1
                elif metrics["status"] == "skipped":
                    skipped += 1
                else:
                    failed += 1
                
                # Log progress
                if (i + 1) % 100 == 0 or (i + 1) == total_files:
                    logger.info(f"PROGRESS: {i+1}/{total_files} files processed ({completed} success, {skipped} skipped, {failed} failed)")
            
            except Exception as e:
                logger.error(f"UNEXPECTED ERROR: Failed to process a file: {e}")
                failed += 1
    
    # Calculate summary statistics
    if not metrics_list:
        logger.error("PROCESS FAILED: No files were processed")
        return
    
    # Calculate average duration for files that were actually processed
    processed_metrics = [m for m in metrics_list if m["status"] == "success"]
    skipped_metrics = [m for m in metrics_list if m["status"] == "skipped"]
    failed_metrics = [m for m in metrics_list if m["status"] == "failed"]
    
    processed_durations = [m["duration_seconds"] for m in processed_metrics]
    avg_duration = sum(processed_durations) / len(processed_durations) if processed_durations else 0
    
    # Log summary
    logger.info("+" + "="*48 + "+")
    logger.info("|" + " SENSOR ETL PROCESS SUMMARY ".center(48) + "|")
    logger.info("+" + "="*48 + "+")
    logger.info(f"| Total files found:           {total_files:4d} |")
    logger.info(f"| Files processed:             {len(processed_metrics):4d} |")
    logger.info(f"| Files skipped:               {len(skipped_metrics):4d} |")
    logger.info(f"| Errors:                      {len(failed_metrics):4d} |")
    logger.info(f"| Average processing time:    {avg_duration:6.2f}s |")
    logger.info("+" + "="*48 + "+")
    
    # Log the slowest files for optimization purposes
    if processed_metrics:
        slowest_files = sorted(processed_metrics, key=lambda x: x["duration_seconds"], reverse=True)[:5]
        logger.info("Slowest files to process:")
        for m in slowest_files:
            logger.info(f"  - {m['file_name']} [Thread-{m['thread_id']}]: {m['duration_seconds']:.2f}s")
    
    # Count number of unique threads used
    thread_ids = set(m.get('thread_id', 0) for m in metrics_list)
    logger.info(f"Number of threads used: {len(thread_ids)}")
    
    logger.info("COMPLETED: Sensor Data ETL process")


def main():
    clean_sensor_data()


if __name__ == "__main__":
    main()