from dotenv import load_dotenv
import os
from algoliasearch.search_client import SearchClient
from scraping_list import create_airline_json

# Load environment variables from .env file
load_dotenv()

# Get Algolia application ID and API key from environment variables
application_id = os.getenv('ALGOLIA_APPLICATION_ID')
api_key = os.getenv('ALGOLIA_API_KEY')

# Initialize Algolia client
client = SearchClient.create(application_id, api_key)

# Create Algolia index and add records
index = client.init_index('squawkit_scraping')

# Data from functions
url = 'https://www.pilotjobsnetwork.com/'
records = create_airline_json(url)
index.save_objects(records,{'autoGenerateObjectIDIfNotExist': True})

