import os
import configparser
from pathlib import Path


class Config:
    """Configuration handler for data cleaning scripts"""
    
    # Mapping between sensor names and apartment identifiers
    APARTMENT_MAPPING = {
        "JeremieVianin": "Apartment_1",
        "JimmyLoup": "Apartment_2"
    }
    
    @classmethod
    def load(cls):
        """Load configuration from config.ini file"""
        # Load basic config from file
        config = configparser.ConfigParser()
        config_path = 'config.ini'
        
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        config.read(config_path)
        
        # Load path settings from config file
        cls.CLEAN_SENSOR_ROOT = Path(config.get('Paths', 'CLEAN_SENSOR_ROOT'))
        cls.CLEAN_WEATHER_ROOT = Path(config.get('Paths', 'CLEAN_WEATHER_ROOT'))
        cls.RAW_SENSOR_ROOT = Path(config.get('Paths', 'RAW_SENSOR_ROOT'))
        cls.RAW_WEATHER_ROOT = Path(config.get('Paths', 'RAW_WEATHER_ROOT'))
        cls.SENSOR_LOG_DIR = Path(config.get('Paths', 'SENSOR_LOG_DIR'))
        cls.WEATHER_LOG_DIR = Path(config.get('Paths', 'WEATHER_LOG_DIR'))
        cls.SENSOR_MAX_WORKERS = int(config.get('Workers', 'SENSOR_MAX_WORKERS'))
        cls.WEATHER_MAX_WORKERS = int(config.get('Workers', 'WEATHER_MAX_WORKERS'))
        
        # Validate required settings
        cls._validate_config()
        
        return cls
    
    @classmethod
    def _validate_config(cls):
        """Validate that required configuration parameters are set"""
        required_attrs = [
            'CLEAN_SENSOR_ROOT', 
            'CLEAN_WEATHER_ROOT', 
            'RAW_SENSOR_ROOT', 
            'RAW_WEATHER_ROOT',
            'SENSOR_LOG_DIR',
            'WEATHER_LOG_DIR',
            'SENSOR_MAX_WORKERS',
            'WEATHER_MAX_WORKERS'
        ]
        
        missing = []
        for attr in required_attrs:
            value = getattr(cls, attr, None)
            if not value:
                missing.append(attr)
        
        if missing:
            raise ValueError(f"Missing required configuration values: {', '.join(missing)}")
            
        return True


# Common utility functions
def ensure_directory(directory_path):
    """Creates a folder if it doesn't exist"""
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        return True
    return False