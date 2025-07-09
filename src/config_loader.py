import configparser
import os

def loadConfig ():
    basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

    config_path= os.path.join(basedir, 'config', 'settings.ini')

    config= configparser.ConfigParser()
    config.read(config_path)

    try:
        # Database Settings
        db_name = config.get('Database', 'db_name')
        
        # Path Settings
        csv_directory = os.path.join(basedir, config.get('Paths', 'raw_data_dir'))
        reports_directory = os.path.join(basedir, config.get('Paths', 'reports_dir'))
        processed_directory = os.path.join(basedir, config.get('Paths', 'archive_dir'))
        log_filename = config.get('Paths', 'log_file_name')
        log_file_path = os.path.join(processed_directory, log_filename)
        
        for path in [csv_directory, reports_directory, processed_directory]:
            os.makedirs(path, exist_ok=True)
        
        return {
            'basedir':basedir,
            'config_file': config_path,
            'db_name': db_name,
            'csv_directory': csv_directory,
            'reports_directory': reports_directory,
            'processed_directory': processed_directory,
            'log_file_path': log_file_path
        }
        
    except Exception as e:
        print (f"Error load configuration file: {e}.")

