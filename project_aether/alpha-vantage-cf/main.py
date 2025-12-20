"""
A Cloud Function to retrieve data from Alpha Vantage API and upload to the dedicated
GCS bucket as a JSON file.
"""
import json
import logging
import os
from datetime import datetime

import requests

from gcp_utils import read_gcp_secret, upload_dict_to_gcs

logging.basicConfig(level=logging.INFO)

PROJECT_ID = os.environ.get('PROJECT_ID')
ALPHA_VANTAGE_API_KEY = os.environ.get('ALPHA_VANTAGE_API_KEY')
OUTPUT_GCS_BUCKET = os.environ.get('OUTPUT_GCS_BUCKET')


def ingest_alpha_vantage_data(request) -> str:
    """
    Cloud Function to retrieve data from Alpha Vantage API and upload to GCS bucket.

    Args:
        request (flask.Request): flask.Request object. Defined as convention for Cloud Functions.

    Returns:
        str: status message.
    """
    # Log the request body for info purposes
    request_body = request.get_json()
    logging.info(f'Request: {request_body}')

    company = request_body.get('company')

    if not company:
        error_message = 'No company provided in the request body.'
        logging.error(error_message)

        return json.dumps({"message": error_message, "error_code": 400})

    api_key = read_gcp_secret(project_id=PROJECT_ID,
                              secret_id=ALPHA_VANTAGE_API_KEY)
    url = (f"https://www.alphavantage.co/query?function="
           f"TIME_SERIES_DAILY&symbol={company}&apikey={api_key}")
    response = requests.get(url=url)

    if response.status_code == 200:
        logging.info('API data retrieved successfully.')

        last_100_days = response.json().get('Time Series (Daily)', {})

        upload_dict_to_gcs(data_dict=last_100_days,
                           bucket_name=OUTPUT_GCS_BUCKET,
                           file_name=f'{company}_daily_{datetime.now().date()}.json')

        success_message = 'Successfully retrieved data from Alpha Vantage API and uploaded to GCS.'
        logging.info(success_message)

        return json.dumps({"message": success_message, "status_code": 200})
    else:
        error_message = f'Failed to retrieve the API data. Status code: {response.status_code}'
        logging.error(error_message)

        return json.dumps({"message": error_message, "error_code": response.status_code})
