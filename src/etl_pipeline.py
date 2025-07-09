#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import fnmatch
import logging
import os
from datetime import datetime
import shutil

from config_loader import loadConfig

settings = loadConfig()

DB_NAME= settings['db_name']
CSV_DIRECTORY = settings['csv_directory']
REPORTS_DIRECTORY = settings['reports_directory']
ARCHIVE_DIRECTORY = settings['processed_directory']
LOG_FILE_PATH= settings['log_file_path']

try:
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers = [
            logging.FileHandler(LOG_FILE_PATH, encoding= 'utf-8', mode='a'), # Log to file
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)
    logger.info(f"Logging configured to write to: {LOG_FILE_PATH}")
    print(f"Logging configured to write to: {LOG_FILE_PATH}")
except Exception as e:
    print(f"FATAL ERROR: Could not configure logging to '{LOG_FILE_PATH}'. Error: {e}")
    exit(1)

def loadData (df: pd.DataFrame, table_name: str, engine):
    if table_name == 'products':
        # Handle missing values and correcting data types for 'products'
        df= df.replace({' ' : pd.NA, '': pd.NA}).copy() # Copy refers to a new dataframe, preventing issues

        df['price'] = pd.to_numeric(df['price'], errors= 'coerce')
        df['price']= df['price'].abs()

        df['name'] = df['name'].str.strip().str.title().fillna('')
        df['category'] = df['category'].str.strip().str.title().fillna('')
     
    elif table_name == 'sales':
        df= df.replace({' ' : pd.NA, '': pd.NA}).copy() # Copy refers to a new dataframe, preventing issues
        
        df['quantity'] = pd.to_numeric(df['quantity'], errors= 'coerce')
        df['sales_date'] = pd.to_datetime(df['sales_date'], errors='coerce', dayfirst=False)

    # --- Load dataframe into table_name ---
    try:
        df.to_sql(table_name, con=engine, if_exists= 'replace', index= False) # won't add index columns to SQL
        logger.info(f"Sucessfully inserted data into table: {table_name}.")
    except SQLAlchemyError as e:
        logger.error(f"Failed to load data into table {table_name}: {e}")
        print(f"Failed to load data into table {table_name}: {e}") 

def readData(directory: str = CSV_DIRECTORY):
    if not os.path.exists(directory):
        logger.error(f"Error: Directory '{directory}' does not exist. Please ensure your CSV files are in this location.")
        print(f"Error: Directory '{directory} does not exists. Please ensure your CSV fies are in this location.'")
        return None
        
    database_file_path= os.path.join(directory, DB_NAME)

    try:
        engine= create_engine(f'sqlite:///{database_file_path}', echo=False)
        logger.info(f"Sucessfully created engine for database: {database_file_path}")
        print(f"Sucessfully created engine for database: {database_file_path}")
        
        for file in os.listdir(directory):
            if fnmatch.fnmatch(file, '*.csv'):
                table_name= file.split(".")[0]
                fullpath = os.path.join(directory, file)

                logger.info(f"Processing: '{file}' from path '{fullpath}' for table '{table_name}'")
                print(f"Processing file...")
                
                try:
                    df= pd.read_csv(fullpath)
                    logger.info(f"Read {len(df)} rows from '{file}'.")
                    
                    df_cleaned= df.drop_duplicates()
                    logger.info(f"Dropped {len(df) - len(df_cleaned)} duplicate rows from {file}. Remaining rows: {len(df_cleaned)}.")
                    
                    loadData(df_cleaned, table_name, engine)
                    shutil.move(fullpath, ARCHIVE_DIRECTORY)
                    
                except pd.errors.EmptyDataError as e:
                    logger.warning(f"CSV file '{file}' is empty. Skipping.")
                    print (f"CSV file '{file}' is empty. Skipping.")                      
                except FileNotFoundError as e:
                    logger.error(f"{e}")
                    print(f"{e}")
                except Exception as e:
                    logger.error(f"Error processing file '{file}': {e}")
                    print(f"Error processing file '{file}': {e}")
                    
        print("\n All CSV files processed and loaded.")            
        return engine
        
    except SQLAlchemyError as e:
        logger.error(f"Failed to open or connect to database: {e}")
        print(f"Failed to open or connect to database: {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occured during readData: {e}")
        print(f"An unexpected error occured during readData: {e}")
        return None

def queryData (engine):
    if engine is None:
        logger.error("Cannot query data: Database engine is not available.")
        print("Cannot query data: Database engine is not available.")
        return
        
    logger.info("\n--- Starting querying and report generation ---")

    if not os.path.exists(REPORTS_DIRECTORY):
        try: 
            os.makedirs(REPORTS_DIRECTORY)
            print(f"Created directory: {REPORTS_DIRECTORY}")
        except OSError as e:
            logger.error(f"Error creating reports directory '{REPORTS_DIRECTORY}': {e}")
            return
            
    try: 
        logger.info("Executing SQL query (1): Total Sales per Product")
        total_sales_query = text(""" 
            SELECT 
                name, 
                ROUND(SUM(a.price * quantity), 2) AS Total_Sales 
            FROM 
                products a 
            INNER JOIN 
                sales b ON a.product_id = b.product_id 
            GROUP BY 
                name
            ORDER BY 
                Total_Sales DESC; 
        """)
        
        df_total_sales= pd.read_sql(total_sales_query, con=engine)
        df_total_sales.to_csv(os.path.join(REPORTS_DIRECTORY, 'total_sales.csv'), index= False)

        logger.info("Executing SQL query using pandas (2): Average price of sold products per Category")
        avg_price_query = text(""" 
            SELECT 
                category, 
                ROUND(AVG(a.price), 2) as Avg_Price_Per_Unit_Sold,
                ROUND(SUM(a.price * b.quantity) / SUM(b.quantity), 2) As Weighted_Avg_Price_Sold
            FROM 
                products a 
            JOIN 
                sales b ON a.product_id = b.product_id 
            GROUP BY 
                category
            ORDER BY Weighted_Avg_Price_Sold DESC;
        """)
        
        df_avg_price_query = pd.read_sql(avg_price_query, con= engine)
        df_avg_price_query.to_csv(os.path.join(REPORTS_DIRECTORY, 'avg_price_per_category.csv'), index= False)
        
        logger.info("Executing SQL query using pandas (3): Best-Selling Category")
        best_selling_query = text(""" 
            SELECT 
                category, 
                ROUND(SUM(a.price * quantity), 2)  as Total_Sales 
            FROM 
                products a
            INNER JOIN 
                sales b ON a.product_id = b.product_id
            GROUP BY 
                category 
            ORDER BY 
                Total_sales DESC 
            LIMIT 1;
        """)
        
        df_best_selling = pd.read_sql(best_selling_query, con=engine)
        df_best_selling.to_csv(os.path.join(REPORTS_DIRECTORY, 'best_selling_category.csv'), index= False)
        
    except SQLAlchemyError as e:
        logger.error(f"Failed to query database: {e}")
        print (f"Failed to query database: {e}")  

if __name__ == "__main__":
    logger.info("--- ETL Process Started ---")
    print("--- ETL Process Started ---")
    
    # Read data from CSVs and load into database
    # readData returns engine object
    db_engine = readData(directory=CSV_DIRECTORY)

    if db_engine:
        queryData(engine=db_engine)
    else:
        logger.error("ETL process failed during data loading. Skipping queries.")
        print("ETL process failed during data loading. Skipping queries.")

    logger.info("--- ETL Process Finished ---")
    print("--- ETL Process Finished ---")
    logging.shutdown()