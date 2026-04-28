import os
import socket
import re
from datetime import datetime
import traceback
import concurrent.futures
import threading
from queue import Queue
from smb.SMBConnection import SMBConnection
from config import Config, ensure_directory, file_exists


def extract_date_components(filename):
    """
    Extracts date components (year, month, day) from a filename.
    Returns (year, month, day) as strings
    """
    match = Config.DATE_PATTERN.search(filename)
    if match:
        raw_date = match.group(1)
        # Convert date to extract year, month, day
        date_obj = datetime.strptime(raw_date, "%d.%m.%Y")
        year = date_obj.strftime("%Y")
        month = date_obj.strftime("%m")
        day = date_obj.strftime("%d")
        return year, month, day
    return "Unknown_Year", "Unknown_Month", "Unknown_Day"


def parse_unc_path(unc_path):
    """Parse a UNC path into server and share components"""
    # Clean up UNC path first
    unc_path = unc_path.replace('\x00', '').strip()
    
    # Handle both \\server\share and server\share formats
    if unc_path.startswith('\\\\'):
        unc_path = unc_path[2:]
    elif unc_path.startswith('\\'):
        unc_path = unc_path[1:]
        
    # Split into server and share
    parts = unc_path.split('\\', 1)
    
    if len(parts) > 1:
        return parts[0], parts[1]
    elif len(parts) == 1:
        return parts[0], ""
    else:
        return "", ""


def get_destination_folder(filename):
    """Determine the destination folder based on the filename"""
    destination_folder = "Unknown"
    for key, folder in Config.APARTMENT_MAPPING.items():
        if key in filename:
            destination_folder = folder
            break
    return destination_folder


def download_file(conn, share, file_info, base_directory):
    """Download a single file via SMB connection"""
    try:
        filename = file_info.filename
        thread_id = threading.get_ident()
        
        # Select the correct folder based on person name in file
        destination_folder = get_destination_folder(filename)
                
        # Extract date and create folder structure
        year, month, day = extract_date_components(filename)
        year_directory = os.path.join(base_directory, destination_folder, year)
        month_directory = os.path.join(year_directory, month)
        day_directory = os.path.join(month_directory, day)
        
        # Create directories
        ensure_directory(day_directory)
        
        # Define local path
        local_path = os.path.join(day_directory, filename)
        
        # Skip if file exists
        if file_exists(local_path):
            print(f"[Thread-{thread_id}] File {filename} already exists. Skipping.")
            return False
            
        # Download file
        print(f"[Thread-{thread_id}] Downloading {filename}...")
        with open(local_path, "wb") as local_file:
            conn.retrieveFile(share, filename, local_file)
            
        print(f"[Thread-{thread_id}] Successfully downloaded: {filename}")
        return True
        
    except Exception as e:
        print(f"[Thread-{thread_id}] Failed to download {file_info.filename}: {e}")
        return False


def download_worker(queue, results, host, share, username, password, base_directory):
    """Worker thread to download files from the queue"""
    thread_id = threading.get_ident()
    
    try:
        # Create a new SMB connection for this thread
        conn = SMBConnection(
            username,
            password,
            f"thread-{thread_id}",  # Client machine name (unique per thread)
            host,
            use_ntlm_v2=True,
            is_direct_tcp=True
        )
        
        if not conn.connect(host, 445):
            print(f"[Thread-{thread_id}] Failed to connect to {host}")
            return
            
        print(f"[Thread-{thread_id}] Connected to {host}, share {share}")
        
        # Process files from the queue
        while not queue.empty():
            try:
                file_info = queue.get(block=False)
                
                # Skip directory entries
                if file_info.filename in [".", ".."] or not file_info.filename.endswith(".json"):
                    queue.task_done()
                    continue
                
                # Download the file
                success = download_file(conn, share, file_info, base_directory)
                if success:
                    results['downloaded'] += 1
                else:
                    results['skipped'] += 1
                
                # Mark task as done
                queue.task_done()
            except Exception as e:
                print(f"[Thread-{thread_id}] Error processing queue item: {e}")
        
        # Close connection
        conn.close()
        
    except Exception as e:
        print(f"[Thread-{thread_id}] Worker error: {e}")


def fetch_sensor_data():
    """Fetch sensor data files via SMB with multi-threading"""
    try:
        # Load configuration
        Config.load()
        
        # Ensure base directory exists
        ensure_directory(Config.SENSORS_DIR)
        
        # Clean up config values and parse UNC path if provided
        host = Config.REMOTE_HOST.replace('\x00', '')
        share = Config.SMB_SHARE.replace('\x00', '')
        username = Config.USERNAME.replace('\x00', '')
        password = Config.PASSWORD.replace('\x00', '')
        
        # Check if the host or share looks like a UNC path
        if '\\' in host:
            print(f"Host appears to be a UNC path: {host}")
            parsed_host, parsed_share = parse_unc_path(host)
            if parsed_host:
                print(f"Parsed UNC path - Host: {parsed_host}, Share: {parsed_share}")
                host = parsed_host
                if parsed_share and not share:
                    share = parsed_share
        
        if '\\' in share:
            print(f"Share appears to contain path separators: {share}")
            _, parsed_share = parse_unc_path(share)
            if parsed_share:
                print(f"Extracted share name: {parsed_share}")
                share = parsed_share
        
        # Debug output
        print(f"Connecting to host: {host}")
        print(f"Using share name: {share}")
        print(f"Using username: {username}")
        
        # Connect to SMB to get file list
        conn = SMBConnection(
            username,
            password,
            socket.gethostname(),
            host,
            use_ntlm_v2=True,
            is_direct_tcp=True
        )
        
        if not conn.connect(host, 445):
            print(f"Failed to connect to {host}")
            return
            
        # Try to list available shares to help with debugging
        try:
            print("Available shares:")
            shares = conn.listShares()
            for s in shares:
                print(f"  - {s.name} ({s.type}): {s.comments}")
        except Exception as e:
            print(f"Unable to list shares: {e}")
        
        # Try to list files from the share
        files = []
        try:
            print(f"Listing files from share: '{share}'")
            files = conn.listPath(share, '/')
            print(f"Successfully listed files from share: '{share}'")
        except Exception as e:
            print(f"Failed to list files from share '{share}': {e}")
            # If original share name fails, try using one of the listed shares
            try:
                shares = conn.listShares()
                if shares:
                    for s in shares:
                        if s.type == 0:  # Only try disk shares
                            try:
                                print(f"Trying alternative share: '{s.name}'")
                                files = conn.listPath(s.name, '/')
                                share = s.name
                                print(f"Successfully listed files using share: '{share}'")
                                break
                            except:
                                continue
            except:
                print("Could not find a working share")
                conn.close()
                return
        
        # Close the initial connection as each worker will create its own
        conn.close()
        
        # Filter for JSON files
        json_files = [file for file in files if file.filename.endswith(".json") and file.filename not in [".", ".."]]
        
        print(f"Found {len(json_files)} JSON files to process")
        
        if not json_files:
            print("No files to download")
            return
        
        # Create a queue for file downloads
        download_queue = Queue()
        for file_info in json_files:
            download_queue.put(file_info)
        
        # Create shared results dictionary
        results = {'downloaded': 0, 'skipped': 0}
        
        # Start worker threads
        threads = []
        max_workers = Config.SENSOR_MAX_WORKERS
        print(f"Starting {max_workers} download threads")
        
        for _ in range(max_workers):
            thread = threading.Thread(
                target=download_worker,
                args=(download_queue, results, host, share, username, password, Config.SENSORS_DIR)
            )
            thread.daemon = True
            thread.start()
            threads.append(thread)
        
        # Wait for all downloads to complete
        for thread in threads:
            thread.join()
        
        print(f"Sensor data fetch completed. Downloaded {results['downloaded']} files, skipped {results['skipped']} files.")
        
    except Exception as e:
        print(f"Error fetching sensor data: {e}")
        traceback.print_exc()
        

if __name__ == "__main__":
    fetch_sensor_data()