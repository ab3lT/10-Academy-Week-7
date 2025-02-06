import pandas as pd
import re
import os
import emoji
import logging

class DataCleaner:
    # Constants for column names
    CHANNEL_USERNAME = 'Channel Username'
    MESSAGE = 'Message'
    DATE = 'Date'
    ID = 'ID'

    def __init__(self):
        """
        Initialize the DataCleaner with a custom logger instance.
        """
        # Configure logging
        logging.basicConfig(
            filename='../data/cleaner_log.log',
            filemode='a',  # Append mode
            format='%(asctime)s - %(levelname)s - %(message)s',
            level=logging.INFO
        )
        self.logger = logging.getLogger(__name__)  # Create a named logger

        # Regex for allowed characters
        self.allowed_characters = re.compile(r'[^a-zA-Z0-9\s.,!?;:()\[\]@&]+')

    def load_data(self, file_path):
        """Loads data from a CSV file."""
        try:
            df = pd.read_csv(file_path)
            self.logger.info(f"Data loaded successfully. Shape: {df.shape}")
            return df
        except FileNotFoundError:
            self.logger.error("File not found. Please check the file path.")
            return pd.DataFrame()  # Return empty DataFrame for consistency
        except Exception as e:
            self.logger.error(f"An error occurred while loading data: {str(e)}")
            return pd.DataFrame()

    def remove_duplicates(self, df, image_directory):
        """Removes duplicate entries and corresponding images."""
        duplicates = df[df.duplicated(subset=self.ID, keep='first')]
        df = df.drop_duplicates(subset=self.ID, keep='first')

        self.logger.info(f"Duplicates removed. New shape: {df.shape}")
        self._remove_duplicate_images(duplicates, image_directory)

        return df

    def _remove_duplicate_images(self, duplicates, image_directory):
        """Removes images corresponding to duplicate entries."""
        for _, row in duplicates.iterrows():
            channel_username = row[self.CHANNEL_USERNAME]
            message_id = row[self.ID]
            image_name = f"{channel_username}_{message_id}.jpg"
            image_path = os.path.join(image_directory, image_name)

            if os.path.exists(image_path):
                try:
                    os.remove(image_path)
                    self.logger.info(f"Removed duplicate image: {image_path}")
                except Exception as e:
                    self.logger.error(f"Error removing image: {image_path}. Exception: {str(e)}")

    def handle_missing_values(self, df):
        """Handles missing values in the dataset."""
        df.fillna({
            self.CHANNEL_USERNAME: 'Unknown',
            self.MESSAGE: 'N/A',
            self.DATE: '1970-01-01 00:00:00'
        }, inplace=True)
        self.logger.info("Missing values handled.")
        return df

    def standardize_formats(self, df):
        """Standardizes the formats of columns like Date, message content, and channel name."""
        # Convert Date column
        if self.DATE in df.columns:
            df[self.DATE] = pd.to_datetime(df[self.DATE], errors='coerce')

        # Clean and format message content
        if self.MESSAGE in df.columns:
            df[self.MESSAGE] = df[self.MESSAGE].apply(self.clean_message_content).str.lower().str.strip()

        # Clean and format channel names
        if self.CHANNEL_USERNAME in df.columns:
            df[self.CHANNEL_USERNAME] = df[self.CHANNEL_USERNAME].str.replace(r'[^a-zA-Z0-9\s]', '', regex=True).str.strip().str.title()

        self.l
