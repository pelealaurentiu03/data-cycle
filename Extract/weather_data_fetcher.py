import os
import paramiko
import traceback
import concurrent.futures
from queue import Queue
import threading
from config import Config, ensure_directory, file_exists


def download_file(sftp, remote_file, local_path):
    """Download a single file via SFTP"""
    try:
        # Skip if file exists
        if file_exists(local_path):
            print(f"[Thread-{threading.get_ident()}] File {remote_file} already exists. Skipping.")
            return False
            
        # Download file
        sftp.get(remote_file, local_path)
        print(f"[Thread-{threading.get_ident()}] Downloaded: {remote_file}")
        return True
    except Exception as e:
        print(f"[Thread-{threading.get_ident()}] Error downloading {remote_file}: {e}")
        return False


def download_worker(queue, results, ssh_client, weather_dir):
    """Worker thread to download files from the queue"""
    try:
        # Open a new SFTP session for this thread
        sftp = ssh_client.open_sftp()
        
        # Navigate to weather folder
        try:
            sftp.chdir("/Meteo2")
        except IOError:
            print(f"[Thread-{threading.get_ident()}] Remote directory /Meteo2 not found")
            sftp.close()
            return
        
        # Process files from the queue
        while not queue.empty():
            try:
                filename = queue.get(block=False)
                local_path = os.path.join(weather_dir, filename)
                
                # Download the file
                success = download_file(sftp, filename, local_path)
                if success:
                    results['downloaded'] += 1
                else:
                    results['skipped'] += 1
                    
                # Mark task as done
                queue.task_done()
            except Exception as e:
                print(f"[Thread-{threading.get_ident()}] Error processing queue item: {e}")
            
        # Close SFTP session
        sftp.close()
    except Exception as e:
        print(f"[Thread-{threading.get_ident()}] Worker error: {e}")


def fetch_weather_data():
    """Fetch weather data files via SFTP with multi-threading"""
    try:
        # Load configuration
        Config.load()

        # Ensure base directory exists
        ensure_directory(Config.WEATHER_DIR)
        
        # Setup SSH client
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # Connect to SFTP
        ssh_client.connect(
            hostname=Config.REMOTE_HOST.replace('\x00', ''),
            port=22,
            username=Config.USERNAME.replace('\x00', ''),
            password=Config.PASSWORD.replace('\x00', ''),
            timeout=120
        )
        
        print(f"Connected to remote host: {Config.REMOTE_HOST}")

        # Open SFTP session to list files
        sftp = ssh_client.open_sftp()
        
        # Navigate to weather folder
        try:
            sftp.chdir("/Meteo2")
        except IOError:
            print("Remote directory /Meteo2 not found")
            sftp.close()
            ssh_client.close()
            return
            
        # List files
        remote_files = sftp.listdir()
        sftp.close()  # Close this session as workers will open their own
        
        # Filter for weather prediction files
        weather_files = [
            filename for filename in remote_files 
            if filename.startswith("Pred_") and filename.endswith(".csv")
        ]
        
        print(f"Found {len(weather_files)} weather files to process")
        
        if not weather_files:
            print("No files to download")
            ssh_client.close()
            return
        
        # Create a queue for file downloads
        download_queue = Queue()
        for filename in weather_files:
            download_queue.put(filename)
        
        # Create shared results dictionary
        results = {'downloaded': 0, 'skipped': 0}
        
        # Start worker threads
        threads = []
        max_workers = Config.WEATHER_MAX_WORKERS
        print(f"Starting {max_workers} download threads")
        
        for _ in range(max_workers):
            thread = threading.Thread(
                target=download_worker,
                args=(download_queue, results, ssh_client, Config.WEATHER_DIR)
            )
            thread.daemon = True
            thread.start()
            threads.append(thread)
        
        # Wait for all downloads to complete
        for thread in threads:
            thread.join()
            
        # Close SSH connection
        ssh_client.close()
        
        print(f"Weather data fetch completed. Downloaded {results['downloaded']} files, skipped {results['skipped']} files.")
        
    except Exception as e:
        print(f"Error fetching weather data: {e}")
        traceback.print_exc()
        

if __name__ == "__main__":
    fetch_weather_data()