import mysql.connector
import datetime
import requests
import configparser
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())

# Load configuration values from a file
config = configparser.ConfigParser()
config.read('config.ini')

# Connect to MySQL database
cnx = mysql.connector.connect(user=config['MYSQL']['user'], password=config['MYSQL']['password'],
                              host=config['MYSQL']['host'],
                              database=config['MYSQL']['database'])

# Create cursor
cursor = cnx.cursor()

while True:
    # Select last_seen and uuid from table
    query = "SELECT last_seen, uuid FROM " + config['MYSQL']['table_name']
    cursor.execute(query)

    # Get the time difference threshold and API call frequency from the config file
    time_diff_threshold = int(config['APP']['time_diff_threshold'])
    api_call_frequency = int(config['APP']['api_call_frequency'])

    # Fetch all data from the cursor
    data = cursor.fetchall()

    # Get current time
    current_time = datetime.datetime.now()

    # Create a list to store uuids that need to send API request
    uuid_to_send = []

    # Iterate through the results
    for (last_seen, uuid) in data:
        # Convert last_seen to datetime object
        last_seen_datetime = datetime.datetime.fromtimestamp(int(last_seen))

        # Calculate time difference
        time_diff = current_time - last_seen_datetime

        # Check if time difference is greater than the threshold
        if time_diff.total_seconds() > time_diff_threshold:
            logging.info(f'Device {uuid} has last seen {time_diff.total_seconds()} seconds ago, which is greater than threshold')
            uuid_to_send.append(uuid)
        else:
            logging.info(f'Device {uuid} has last seen {time_diff.total_seconds()} seconds ago, which is less than threshold')

    # Close cursor and connection
    cursor.close()
        cnx.close()

    # Send API requests in parallel
    def send_request(uuid):
        requests.get(config['APP']['api_url'] + "restart_phone?origin=" + uuid + "&adb=False")
        return uuid

    # Send API requests in parallel
    with ThreadPoolExecutor() as executor:
        results = [executor.submit(send_request, uuid) for uuid in uuid_to_send]
    for f in as_completed(results):
        uuid = f.result()
        logging.info(f'API request sent for {uuid}')

    time.sleep(600)

