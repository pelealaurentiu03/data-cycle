import os
import configparser
from pathlib import Path


class Config:
    
    @classmethod
    def load(cls):
        # Load basic config from file
        config = configparser.ConfigParser()
        config_path = 'config.ini'
        
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        config.read(config_path)
        
        # Load path settings from config file
        cls.SENSORS_DATA_DIR = Path(config.get('Paths', 'SENSORS_DATA_DIR'))
        cls.WEATHER_DATA_DIR = Path(config.get('Paths', 'WEATHER_DATA_DIR'))
        
        # Validate required settings
        cls._validate_config()
        
        return cls
    
    @classmethod
    def _validate_config(cls):
        required_attrs = [
            'SENSORS_DATA_DIR', 
            'WEATHER_DATA_DIR'
        ]
        
        missing = []
        for attr in required_attrs:
            value = getattr(cls, attr, None)
            if not value:
                missing.append(attr)
        
        if missing:
            raise ValueError(f"Missing required configuration values: {', '.join(missing)}")
            
        return True


def ensure_directory(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        return True
    return False