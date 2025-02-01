import os
import csv
import asyncio
from dotenv import load_dotenv
from telethon import TelegramClient
import logging 

load_dotenv('../.env')

class TelegramChannelScraper:
    def __init__(self, api_id, api_hash, session_name, media_dir='../data/photos', 
                 csv_file='../data/telegram_data.csv', channels=None, log_file='../data/scraper.log'):
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_name = session_name
        self.media_dir = media_dir
        self.csv_file = csv_file
        self.channels = channels or []

        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
        )
        self.logger = logging.getLogger(__name__)
        os.makedirs(self.media_dir, exist_ok=True)

    async def scrape_channel(self, client, channel_username, writer):
        try:
            entity = await client.get_entity(channel_username)
            channel_title = entity.title
            channel_id = entity.id
            self.logger.info(f"Scraping {channel_username}...")

            async for message in client.iter_messages(entity, limit=None):
                media_path = await self.download_media(client, message, channel_username)
                writer.writerow([channel_title, channel_username, channel_id, message.id, message.message, message.date, media_path])
            
            self.logger.info(f"Finished scraping {channel_username}")
        except Exception as e:
            self.logger.error(f"Error scraping {channel_username}: {e}")

    async def download_media(self, client, message, channel_username):
        if message.media and hasattr(message.media, 'photo'):
            filename = f"{channel_username}_{message.id}.jpg"
            media_path = os.path.join(self.media_dir, filename)
            await client.download_media(message.media, media_path)
            return media_path
        return None

    async def run(self):
        self.logger.info("Starting scraper...")
        async with TelegramClient(self.session_name, self.api_id, self.api_hash) as client:
            with open(self.csv_file, 'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(['channel_title', 'channel_username', 'channel_id', 'message_id', 'message', 'date', 'media_path'])
                tasks = [self.scrape_channel(client, channel, writer) for channel in self.channels]
                await asyncio.gather(*tasks)
        self.logger.info("Scraping completed.")

if __name__ == "__main__":
    api_id = os.getenv('api_id')
    api_hash = os.getenv('api_hash')
    
    if not api_id or not api_hash:
        raise ValueError("API credentials missing. Check .env file.")
    
    channels_to_scrape = ['@DoctorsET', '@lobelia4cosmetics', '@yetenaweg', '@EAHCI', '@CheMed123']
    scraper = TelegramChannelScraper(api_id, api_hash, '../data/scraping_session', channels=channels_to_scrape)
    
    asyncio.run(scraper.run())
