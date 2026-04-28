"""
Configuration management for the application.
Loads settings from config.ini and secure credentials from Windows Credential Manager.
"""

import os
import configparser
import ctypes
import ctypes.wintypes as wintypes

# Windows Credential Manager API constants
CRED_TYPE_GENERIC = 1
CRED_PERSIST_LOCAL_MACHINE = 2
ERROR_NOT_FOUND = 1168


class CREDENTIAL(ctypes.Structure):
    """Structure for Windows credential"""
    _fields_ = [
        ('Flags', wintypes.DWORD),
        ('Type', wintypes.DWORD),
        ('TargetName', wintypes.LPWSTR),
        ('Comment', wintypes.LPWSTR),
        ('LastWritten', wintypes.FILETIME),
        ('CredentialBlobSize', wintypes.DWORD),
        ('CredentialBlob', ctypes.POINTER(ctypes.c_byte)),
        ('Persist', wintypes.DWORD),
        ('AttributeCount', wintypes.DWORD),
        ('Attributes', ctypes.c_void_p),
        ('TargetAlias', wintypes.LPWSTR),
        ('UserName', wintypes.LPWSTR)
    ]


class CredManager:
    """Windows Credential Manager interface"""
    
    def __init__(self):
        """Initialize the Credential Manager API"""
        self.advapi32 = ctypes.WinDLL('advapi32', use_last_error=True)
        
        # Define function prototypes
        self.advapi32.CredReadW.argtypes = [
            wintypes.LPWSTR, wintypes.DWORD, wintypes.DWORD, 
            ctypes.POINTER(ctypes.POINTER(CREDENTIAL))
        ]
        self.advapi32.CredReadW.restype = wintypes.BOOL
        
        self.advapi32.CredFree.argtypes = [ctypes.POINTER(CREDENTIAL)]
        self.advapi32.CredFree.restype = None
        
    def get_credential(self, target_name):
        """
        Get a credential from Windows Credential Manager
        
        Args:
            target_name (str): The name of the credential to retrieve
            
        Returns:
            tuple: (username, password) if found, (None, None) otherwise
        """
        pcred = ctypes.POINTER(CREDENTIAL)()
        result = self.advapi32.CredReadW(
            target_name, CRED_TYPE_GENERIC, 0, ctypes.byref(pcred)
        )
        
        if not result:
            error_code = ctypes.get_last_error()
            if error_code == ERROR_NOT_FOUND:
                print(f"Credential '{target_name}' not found in Credential Manager")
            else:
                print(f"Error accessing Credential Manager: {error_code}")
            return None, None
        
        try:
            username = pcred.contents.UserName
            cred_blob = pcred.contents.CredentialBlob
            cred_size = pcred.contents.CredentialBlobSize
            
            # Convert the binary blob to string
            password = ''.join(chr(cred_blob[i]) for i in range(cred_size))
            
            return username, password
        finally:
            self.advapi32.CredFree(pcred)


class Config:
    """Centralized configuration settings"""
    
    # Credential target names in Windows Credential Manager
    CRED_TARGET_SERVER = "Database_Server"
    CRED_TARGET_DATABASE = "Database_Name"
    CRED_TARGET_CREDENTIALS = "Database_Credentials"
    
    @classmethod
    def load(cls):
        """Load configuration from config.ini and Windows Credential Manager"""
        # Load basic config from file
        config = configparser.ConfigParser()
        config_path = os.path.join(os.path.dirname(__file__), "config.ini")
        
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        config.read(config_path)
        
        # Load non-sensitive settings from config file
        cls.STATIC_DATA_DIR = config.get('Paths', 'STATIC_DATA_DIR')
        cls.WEATHER_DATA_DIR = config.get('Paths', 'WEATHER_DATA_DIR')
        cls.SENSORS_DATA_DIR = config.get('Paths', 'SENSORS_DATA_DIR')
        cls.ML_FORECASTS_DIR = config.get('Paths', 'ML_FORECASTS_DIR')
        cls.MAX_WORKERS = int(config.get('Workers', 'MAX_WORKERS'))
        
        # Get sensitive data from Windows Credential Manager
        cred_manager = CredManager()
        
        # Get server
        _, server_blob = cred_manager.get_credential(cls.CRED_TARGET_SERVER)
        cls.SERVER = server_blob if server_blob else ""
        
        # Get database name
        _, database_blob = cred_manager.get_credential(cls.CRED_TARGET_DATABASE)
        cls.DATABASE = database_blob if database_blob else ""
        
        # Get username and password
        username, password = cred_manager.get_credential(cls.CRED_TARGET_CREDENTIALS)
        cls.USERNAME = username if username else ""
        cls.PASSWORD = password if password else ""
        
        # Validate required settings
        cls._validate_config()
        
        return cls
    
    @classmethod
    def _validate_config(cls):
        """Validate that required configuration parameters are set"""
        required_attrs = [
            'SERVER', 'DATABASE', 'USERNAME', 'PASSWORD', 
            'STATIC_DATA_DIR', 'WEATHER_DATA_DIR', 'SENSORS_DATA_DIR', 'ML_FORECASTS_DIR', 'MAX_WORKERS'
        ]
        
        missing = []
        for attr in required_attrs:
            value = getattr(cls, attr, None)
            if value is None or value == "":
                missing.append(attr)
        
        if missing:
            raise ValueError(f"Missing required configuration values: {', '.join(missing)}")
        
        # Verify directories exist
        dirs_to_check = [cls.STATIC_DATA_DIR, cls.WEATHER_DATA_DIR, cls.SENSORS_DATA_DIR]
        for dir_path in dirs_to_check:
            if not os.path.exists(dir_path):
                print(f"Warning: Directory does not exist: {dir_path}")
        
        return True


# Common utility functions
def ensure_directory(directory_path):
    """Creates a folder if it doesn't exist"""
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Created directory: {directory_path}")


def file_exists(file_path):
    """Check if a file already exists"""
    return os.path.exists(file_path)


if __name__ == "__main__":
    try:
        config = Config.load()
    except Exception as e:
        print(f"Error loading configuration: {e}")