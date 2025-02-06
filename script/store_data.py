from dotenv import load_dotenv
import os,sys
from sqlalchemy import create_engine
import pandas as pd

from logger import Logger

# Load environment variables from .env file
load_dotenv()
logger = Logger(log_file='../data/database.log')
def conn():
    # Get database credentials from the .env file
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')

    # Create SQLAlchemy engine
    engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

    # Test connection by trying to connect to the engine
    connection = engine.connect()
    print("Connection to database successful!")
    connection.close()  # Close the connection after testing

    return engine  # Return the engine

def store_cleaned_data(cleaned_df, table_name):
    """
    Store the cleaned DataFrame into the specified database table.
    
    Parameters:
    cleaned_df (pd.DataFrame): The cleaned DataFrame to store.
    table_name (str): The name of the table where data will be stored.
    """
    engine = conn()  # Use the engine from the conn function

    try:
        # Store the cleaned DataFrame to the specified table
        cleaned_df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Cleaned data successfully stored in the '{table_name}' table.")
        logger.info(f"Cleaned data successfully stored in the '{table_name}' table.")
    except Exception as e:
        print(f"An error occurred while storing data: {str(e)}")
        logger.info(f"An error occurred while storing data: {str(e)}")
    finally:
        engine.dispose()  # Close the engine connection

# Example usage
if __name__ == '__main__':
    # Assuming you have a cleaned DataFrame called `cleaned_df`
    cleaned_df = pd.read_csv('../data/telegram_data_cleaned.csv')  # Load your cleaned data
    store_cleaned_data(cleaned_df, 'ethio_medical')  # Store it in the database